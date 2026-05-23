using System;
using System.IO.Pipes;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleNamedPipe;

public class PipeClient : IDisposable, IAsyncDisposable
{
    public event EventHandler<ConnectionEventArgs>? Connected;
    public event EventHandler<ConnectionEventArgs>? Disconnected;
    public event EventHandler<MessageReceivedEventArgs>? MessageReceived;
    public event EventHandler<ErrorEventArgs>? Error;
    public event EventHandler<ReconnectingEventArgs>? Reconnecting;

    private readonly string _serverName;
    private readonly string _pipeName;
    private readonly bool _enableClientName;
    private readonly string? _clientName;
    private readonly SemaphoreSlim _connectionSemaphore;
    private readonly SemaphoreSlim _sendSemaphore;
    private readonly IMessageEncoder _encoder;

    private NamedPipeClientStream? _pipeClient;
    private CancellationTokenSource? _connectionCancellationTokenSource;
    private CancellationTokenSource? _autoReconnectCancellationTokenSource;
    private Task? _receiveTask;
    private Task? _autoReconnectTask;
    private volatile bool _isDisposed;
    private volatile bool _isAutoReconnecting;
    private int _disconnectNotified = 1;

    public bool IsConnected => _pipeClient?.IsConnected ?? false;
    public string? ClientName => _clientName;

    public PipeClient(
        string pipeName,
        string serverName = ".",
        string? clientName = null,
        MessageTransmissionMode transmissionMode = MessageTransmissionMode.ByteBasedBigEndian,
        int maxMessageBytes = 1024 * 1024)
    {
        _pipeName = pipeName ?? throw new ArgumentNullException(nameof(pipeName));
        _serverName = serverName ?? throw new ArgumentNullException(nameof(serverName));
        _clientName = clientName;
        _enableClientName = !string.IsNullOrEmpty(clientName);
        _connectionSemaphore = new SemaphoreSlim(1, 1);
        _sendSemaphore = new SemaphoreSlim(1, 1);
        _encoder = MessageEncoderFactory.CreateEncoder(transmissionMode, maxMessageBytes);
    }

    public async Task ConnectAsync(int timeoutMilliseconds = 5000)
    {
        ThrowIfDisposed();

        if (_isAutoReconnecting)
        {
            throw new InvalidOperationException("Cannot manually connect when auto-reconnection is active. Call StopConnectWithAutoReconnectionAsync() first or use Dispose() to stop.");
        }

        await _connectionSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            if (IsConnected)
            {
                throw new InvalidOperationException("Client is already connected");
            }

            await ConnectInternalAsync(timeoutMilliseconds).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            OnError(new ErrorEventArgs("Connection failed", ex));
            throw;
        }
        finally
        {
            _connectionSemaphore.Release();
        }
    }

    public async Task<bool> TryConnectAsync(int timeoutMilliseconds = 5000)
    {
        try
        {
            await ConnectAsync(timeoutMilliseconds).ConfigureAwait(false);
            return IsConnected;
        }
        catch
        {
            return false;
        }
    }

    public void StartConnectWithAutoReconnection()
    {
        ThrowIfDisposed();

        if (_isAutoReconnecting)
        {
            return;
        }

        _isAutoReconnecting = true;
        _autoReconnectCancellationTokenSource = new CancellationTokenSource();
        _autoReconnectTask = AutoReconnectLoopAsync(_autoReconnectCancellationTokenSource.Token);
    }

    public async Task StopConnectWithAutoReconnectionAsync()
    {
        _isAutoReconnecting = false;

        var cts = _autoReconnectCancellationTokenSource;
        if (cts != null)
        {
            cts.Cancel();
        }

        if (!IsConnected)
        {
            _connectionCancellationTokenSource?.Cancel();
        }

        var task = _autoReconnectTask;
        if (task != null)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // 正常停止自动重连。
            }
        }

        cts?.Dispose();
        _autoReconnectCancellationTokenSource = null;
        _autoReconnectTask = null;
    }

    private async Task AutoReconnectLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested && !_isDisposed)
        {
            try
            {
                if (!IsConnected)
                {
                    OnReconnecting(new ReconnectingEventArgs("Connecting to server"));

                    await _connectionSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                    try
                    {
                        if (!IsConnected)
                        {
                            await ConnectInternalAsync(Timeout.Infinite).ConfigureAwait(false);
                        }
                    }
                    finally
                    {
                        _connectionSemaphore.Release();
                    }
                }

                var receiveTask = _receiveTask;
                if (receiveTask != null)
                {
                    var completedTask = await Task.WhenAny(receiveTask, WaitForCancellationAsync(cancellationToken)).ConfigureAwait(false);
                    if (completedTask != receiveTask)
                    {
                        break;
                    }

                    await receiveTask.ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                OnError(new ErrorEventArgs("Reconnection attempt failed", ex));
            }

            try
            {
                await Task.Delay(2000, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private static async Task WaitForCancellationAsync(CancellationToken cancellationToken)
    {
        var completion = new TaskCompletionSource<bool>();
        using (cancellationToken.Register(() => completion.TrySetResult(true)))
        {
            await completion.Task.ConfigureAwait(false);
        }
    }

    private async Task ConnectInternalAsync(int timeoutMilliseconds)
    {
        await CleanupConnectionAsync(notifyDisconnected: false, awaitReceiveTask: false).ConfigureAwait(false);

        _connectionCancellationTokenSource = new CancellationTokenSource();
        _pipeClient = new NamedPipeClientStream(
            _serverName,
            _pipeName,
            PipeDirection.InOut,
            PipeOptions.Asynchronous);

        try
        {
            await _pipeClient.ConnectAsync(timeoutMilliseconds, _connectionCancellationTokenSource.Token).ConfigureAwait(false);
            _pipeClient.ReadMode = _encoder.TransmissionMode;
            Interlocked.Exchange(ref _disconnectNotified, 0);

            if (_enableClientName && !string.IsNullOrEmpty(_clientName))
            {
                await SendMessageInternalAsync($"CLIENTNAME:{_clientName}", _connectionCancellationTokenSource.Token).ConfigureAwait(false);
            }

            OnConnected(new ConnectionEventArgs("Successfully connected to server"));
            _receiveTask = ReceiveMessagesAsync(_connectionCancellationTokenSource.Token);
        }
        catch
        {
            await CleanupConnectionAsync(notifyDisconnected: false, awaitReceiveTask: false).ConfigureAwait(false);
            throw;
        }
    }

    public async Task DisconnectAsync()
    {
        await _connectionSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            await CleanupConnectionAsync(notifyDisconnected: true, awaitReceiveTask: true).ConfigureAwait(false);
        }
        finally
        {
            _connectionSemaphore.Release();
        }
    }

    private async Task CleanupConnectionAsync(bool notifyDisconnected, bool awaitReceiveTask)
    {
        var cts = _connectionCancellationTokenSource;
        _connectionCancellationTokenSource = null;

        if (cts != null)
        {
            try
            {
                cts.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }
        }

        var pipeClient = _pipeClient;
        _pipeClient = null;

        if (pipeClient != null)
        {
            try
            {
                if (pipeClient.IsConnected)
                {
                    pipeClient.Close();
                }
            }
            catch (Exception ex)
            {
                OnError(new ErrorEventArgs("Error closing pipe", ex));
            }
            finally
            {
                pipeClient.Dispose();
            }
        }

        if (awaitReceiveTask)
        {
            var receiveTask = _receiveTask;
            if (receiveTask != null)
            {
                try
                {
                    await receiveTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception ex)
                {
                    OnError(new ErrorEventArgs("Error during disconnect", ex));
                }
            }
        }

        _receiveTask = null;
        cts?.Dispose();

        if (notifyDisconnected)
        {
            NotifyDisconnectedOnce("Disconnected from server");
        }
        else
        {
            Interlocked.Exchange(ref _disconnectNotified, 1);
        }
    }

    public async Task SendMessageAsync(string message, CancellationToken cancellationToken = default)
    {
        if (message == null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        if (message.Length == 0)
        {
            throw new ArgumentException("Message cannot be empty.", nameof(message));
        }

        await _sendSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await SendMessageInternalAsync(message, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _sendSemaphore.Release();
        }
    }

    private async Task SendMessageInternalAsync(string message, CancellationToken cancellationToken)
    {
        if (!IsConnected)
        {
            throw new InvalidOperationException("Client is not connected");
        }

        try
        {
            await _encoder.WriteMessageAsync(_pipeClient!, message, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            OnError(new ErrorEventArgs("Error sending message", ex));
            await CleanupConnectionAsync(notifyDisconnected: true, awaitReceiveTask: false).ConfigureAwait(false);
            throw;
        }
    }

    private async Task ReceiveMessagesAsync(CancellationToken cancellationToken)
    {
        var disconnectedByError = false;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var pipeClient = _pipeClient;
                if (pipeClient == null)
                {
                    break;
                }

                string message = await _encoder.ReadMessageAsync(pipeClient, cancellationToken).ConfigureAwait(false);
                if (!string.IsNullOrEmpty(message))
                {
                    OnMessageReceived(new MessageReceivedEventArgs(message));
                }
            }
        }
        catch (OperationCanceledException)
        {
            // 主动断开或释放时的正常路径。
        }
        catch (Exception ex)
        {
            disconnectedByError = true;
            OnError(new ErrorEventArgs("Error receiving message", ex));
        }
        finally
        {
            if (!cancellationToken.IsCancellationRequested || disconnectedByError)
            {
                await CleanupConnectionAsync(notifyDisconnected: true, awaitReceiveTask: false).ConfigureAwait(false);
            }
        }
    }

    protected virtual void OnConnected(ConnectionEventArgs e)
    {
        Connected?.Invoke(this, e);
    }

    protected virtual void OnDisconnected(ConnectionEventArgs e)
    {
        Disconnected?.Invoke(this, e);
    }

    protected virtual void OnMessageReceived(MessageReceivedEventArgs e)
    {
        MessageReceived?.Invoke(this, e);
    }

    protected virtual void OnError(ErrorEventArgs e)
    {
        Error?.Invoke(this, e);
    }

    protected virtual void OnReconnecting(ReconnectingEventArgs e)
    {
        Reconnecting?.Invoke(this, e);
    }

    private void NotifyDisconnectedOnce(string message)
    {
        if (Interlocked.Exchange(ref _disconnectNotified, 1) == 0)
        {
            OnDisconnected(new ConnectionEventArgs(message));
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (_isDisposed)
        {
            return;
        }

        if (disposing)
        {
            DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_isDisposed)
        {
            return;
        }

        await StopConnectWithAutoReconnectionAsync().ConfigureAwait(false);
        await CleanupConnectionAsync(notifyDisconnected: true, awaitReceiveTask: true).ConfigureAwait(false);

        _connectionSemaphore.Dispose();
        _sendSemaphore.Dispose();

        _isDisposed = true;
        GC.SuppressFinalize(this);
    }

    private void ThrowIfDisposed()
    {
        if (_isDisposed)
        {
            throw new ObjectDisposedException(nameof(PipeClient));
        }
    }

    public class ConnectionEventArgs : EventArgs
    {
        public string Message { get; }

        public ConnectionEventArgs(string message)
        {
            Message = message;
        }
    }

    public class ErrorEventArgs : EventArgs
    {
        public string Message { get; }
        public Exception Exception { get; }

        public ErrorEventArgs(string message, Exception exception)
        {
            Message = message;
            Exception = exception;
        }
    }

    public class ReconnectingEventArgs : EventArgs
    {
        public string Message { get; }

        public ReconnectingEventArgs(string message)
        {
            Message = message;
        }
    }

    public class MessageReceivedEventArgs : EventArgs
    {
        public string Message { get; }

        public MessageReceivedEventArgs(string message)
        {
            Message = message;
        }
    }
}
