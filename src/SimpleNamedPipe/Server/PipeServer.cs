using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Security.AccessControl;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleNamedPipe;

public class PipeServer : IDisposable, IAsyncDisposable
{
	// 事件定义
	public event EventHandler<ClientConnectedEventArgs>? ClientConnected;
	public event EventHandler<ClientDisconnectedEventArgs>? ClientDisconnected;
	public event EventHandler<MessageReceivedEventArgs>? MessageReceived;

	private readonly string _pipeName;
	private readonly bool _enableClientName;
	private readonly PipeSecurity? _pipeSecurity;
	private readonly ConcurrentDictionary<int, PipeClientInfo> _activeClients;
	private CancellationTokenSource? _cancellationTokenSource;
	private Task? _listenerTask;
	private NamedPipeServerStream? _pendingPipeServerStream;
	private int _clientCount;
	private bool _isDisposed;

	public bool IsRunning => _listenerTask != null
	                         && !_listenerTask.IsCompleted
	                         && _cancellationTokenSource?.Token.IsCancellationRequested != true;

	private readonly IMessageEncoder _encoder;

	public PipeServer(string pipeName, 
        bool enableClientName = false, 
        MessageTransmissionMode transmissionMode = MessageTransmissionMode.ByteBasedBigEndian,
        PipeSecurity? pipeSecurity = null,
        int maxMessageBytes = 1024 * 1024)
	{
		_pipeName = pipeName ?? throw new ArgumentNullException(nameof(pipeName));
		_enableClientName = enableClientName;
		_pipeSecurity = pipeSecurity;
		_activeClients = new ConcurrentDictionary<int, PipeClientInfo>();
		_clientCount = 0;

		// 使用工厂方法创建编码器
		_encoder = MessageEncoderFactory.CreateEncoder(transmissionMode, maxMessageBytes);
	}

	public Task StartAsync()
	{
		if (IsRunning)
			throw new InvalidOperationException("Server is already running");

		_cancellationTokenSource = new CancellationTokenSource();
		_listenerTask = ListenForClientsAsync(_cancellationTokenSource.Token);
		return Task.CompletedTask;
	}

	public async Task StopAsync()
	{
		if (_listenerTask == null && _activeClients.IsEmpty)
			return;

		_cancellationTokenSource?.Cancel();
		_pendingPipeServerStream?.Dispose();

		try
		{
			if (_listenerTask != null)
				await _listenerTask;
		}
		catch (OperationCanceledException)
		{
			// Expected exception when canceling the task
		}
		catch (Exception ex)
		{
			Debug.WriteLine($"Error stopping pipe server: {ex}");
		}

		// 关闭所有客户端连接
		var clientIds = _activeClients.Keys.ToList(); // Create a copy of client IDs
		foreach (var clientId in clientIds)
		{
			DisconnectClient(clientId);
		}

		_listenerTask = null;
	}

	public async Task SendMessageAsync(int clientId, string message, CancellationToken cancellationToken = default)
	{
		if (message == null)
			throw new ArgumentNullException(nameof(message));
		if (message.Length == 0)
			throw new ArgumentException("Message cannot be empty.", nameof(message));

		if (!_activeClients.TryGetValue(clientId, out var clientInfo))
			throw new ArgumentException($"Client {clientId} not found");

		if (!clientInfo.PipeStream.IsConnected)
		{
			throw new IOException("连接已断开。");
		}

		await clientInfo.SendSemaphore.WaitAsync(cancellationToken);
		try
		{
			await _encoder.WriteMessageAsync(clientInfo.PipeStream, message, cancellationToken).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			DisconnectClient(clientId);
			throw new IOException($"Error sending message to client {clientId}", ex);
		}
		finally
		{
			clientInfo.SendSemaphore.Release();
		}
	}

	public async Task BroadcastMessageAsync(string message, CancellationToken cancellationToken = default)
	{
		var sendTasks = _activeClients.Select(async client =>
		{
			try
			{
				await SendMessageAsync(client.Key, message, cancellationToken).ConfigureAwait(false);
			}
			catch (Exception ex)
			{
				Debug.WriteLine($"Error broadcasting to client {client.Key}: {ex}");
			}
		});

		await Task.WhenAll(sendTasks);
	}

	private async Task ListenForClientsAsync(CancellationToken cancellationToken)
	{
		while (!cancellationToken.IsCancellationRequested)
		{
			NamedPipeServerStream? pipeServerStream = null;

			try
			{
				pipeServerStream = CreatePipeServerStream();
				_pendingPipeServerStream = pipeServerStream;

				await pipeServerStream.WaitForConnectionAsync(cancellationToken).ConfigureAwait(false);
				_pendingPipeServerStream = null;

				int clientId = Interlocked.Increment(ref _clientCount);
				var clientInfo = new PipeClientInfo(clientId, pipeServerStream);
				_activeClients.TryAdd(clientId, clientInfo);

				OnClientConnected(new ClientConnectedEventArgs(clientInfo));

                // 为每个客户端启动一个处理线程
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                Task.Run(async () =>
                {
                    try
                    {
                        await HandleClientCommunicationAsync(clientInfo, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Debug.WriteLine($"Client handler error: {ex}");
                    }
                });
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            }
			catch (OperationCanceledException)
			{
				pipeServerStream?.Dispose();
				break;
			}
			catch (ObjectDisposedException) when (cancellationToken.IsCancellationRequested)
			{
				break;
			}
			catch (Exception ex)
			{
				pipeServerStream?.Dispose();
				Debug.WriteLine($"Error in ListenForClientsAsync: {ex}");
				// 继续监听，不要因为单个客户端的错误而停止服务器
				await Task.Delay(1000, cancellationToken); // 添加延迟避免过于频繁的重试
			}
			finally
			{
				if (ReferenceEquals(_pendingPipeServerStream, pipeServerStream))
				{
					_pendingPipeServerStream = null;
				}
			}
		}
	}

	private NamedPipeServerStream CreatePipeServerStream()
	{
		if (_pipeSecurity == null)
		{
			return new NamedPipeServerStream(
				_pipeName,
				PipeDirection.InOut,
				NamedPipeServerStream.MaxAllowedServerInstances,
				_encoder.TransmissionMode,
				PipeOptions.Asynchronous);
		}

#if NETFRAMEWORK
		// 允许主程序显式传入 ACL，支持低权限进程连接提权后的主程序。
		return new NamedPipeServerStream(
			_pipeName,
			PipeDirection.InOut,
			NamedPipeServerStream.MaxAllowedServerInstances,
			_encoder.TransmissionMode,
			PipeOptions.Asynchronous,
			0,
			0,
			_pipeSecurity);
#elif NETSTANDARD
		throw new PlatformNotSupportedException("PipeSecurity is not supported by the netstandard target. Use a framework-specific target such as net472 or net10.0-windows.");
#else
		// .NET Core/5+ 上带 ACL 的命名管道通过 AccessControl 扩展创建。
		return NamedPipeServerStreamAcl.Create(
			_pipeName,
			PipeDirection.InOut,
			NamedPipeServerStream.MaxAllowedServerInstances,
			_encoder.TransmissionMode,
			PipeOptions.Asynchronous,
			0,
			0,
			_pipeSecurity);
#endif
	}

	private async Task HandleClientCommunicationAsync(PipeClientInfo clientInfo, CancellationToken cancellationToken)
	{
		try
		{
			
			while (clientInfo.PipeStream.IsConnected && !cancellationToken.IsCancellationRequested)
			{

				string message = await _encoder.ReadMessageAsync(clientInfo.PipeStream, cancellationToken);

				if (!string.IsNullOrEmpty(message))
				{
					// 处理客户端名称消息
					if (_enableClientName && clientInfo.ClientName == null && message.StartsWith("CLIENTNAME:"))
					{
						string clientName = message.Substring(11);
						clientInfo.ClientName = clientName;

						continue; // 不触发消息接收事件
					}

					OnMessageReceived(new MessageReceivedEventArgs(clientInfo, message));
				}
			}
		}
		catch (OperationCanceledException)
		{
			// 正常的取消操作
		}
		catch (Exception ex)
		{
			// 记录错误但继续运行
			Debug.WriteLine($"HandleClientCommunicationAsync error for client {clientInfo.ClientId}: {ex}");
		}
		finally
		{
			DisconnectClient(clientInfo.ClientId);
		}
	}

	public void DisconnectClient(int clientId)
	{
		if (_activeClients.TryRemove(clientId, out var clientInfo))
		{
			try
			{
				if (clientInfo.PipeStream.IsConnected)
					clientInfo.PipeStream.Disconnect();

				clientInfo.Dispose(); // Dispose PipeClientInfo (which disposes stream and semaphore)

				OnClientDisconnected(new ClientDisconnectedEventArgs(clientInfo));
			}
			catch (Exception ex)
			{
				Debug.WriteLine($"Error disconnecting client {clientId}: {ex}"); // Log full exception
				// 忽略关闭时的错误
			}
		}
	}

	protected virtual void OnClientConnected(ClientConnectedEventArgs e)
	{
		ClientConnected?.Invoke(this, e);
	}

	protected virtual void OnClientDisconnected(ClientDisconnectedEventArgs e)
	{
		ClientDisconnected?.Invoke(this, e);
	}

	protected virtual void OnMessageReceived(MessageReceivedEventArgs e)
	{
		MessageReceived?.Invoke(this, e);
	}

	public void Dispose()
	{
		Dispose(true);
		GC.SuppressFinalize(this);
	}

	protected virtual void Dispose(bool disposing)
	{
		if (_isDisposed)
			return;

		if (disposing)
		{
			// Block on DisposeAsync() for the sync Dispose() pattern.
			// This can be problematic in some contexts (e.g. UI thread).
			// Consumers are encouraged to use DisposeAsync() where possible.
			DisposeAsync().AsTask().GetAwaiter().GetResult();
		}
	}

	public async ValueTask DisposeAsync()
	{
		if (_isDisposed)
			return;

		await StopAsync(); // StopAsync should handle client disconnections and clearing _activeClients.
		_cancellationTokenSource?.Dispose();

		// As a safeguard, iterate and dispose any remaining client info objects.
		// StopAsync should ideally clear _activeClients, making this loop a no-op.
		foreach (var clientInfo in _activeClients.Values)
		{
			clientInfo.Dispose(); 
		}
		_activeClients.Clear();

		_isDisposed = true;
		GC.SuppressFinalize(this);
	}

	#region Types


	// 事件参数类
	public class ClientConnectedEventArgs(PipeClientInfo clientInfo) : EventArgs
	{
		public PipeClientInfo ClientInfo { get; } = clientInfo;
	}

	public class ClientDisconnectedEventArgs(PipeClientInfo clientInfo) : EventArgs
	{
		public PipeClientInfo ClientInfo { get; } = clientInfo;
	}

	public class MessageReceivedEventArgs(PipeClientInfo clientInfo, string message) : EventArgs
	{
		public PipeClientInfo ClientInfo { get; } = clientInfo;
		public string Message { get; } = message;
	}

	#endregion
}

