using System.Diagnostics;
using SimpleNamedPipe;

namespace SimpleNamedPipeTests
{
	[TestClass]
	public sealed class PipeTests
	{
		[TestMethod]
		[DataRow(MessageTransmissionMode.MessageBased)]
		[DataRow(MessageTransmissionMode.ByteBasedBigEndian)]
		public async Task CanSendRecv(MessageTransmissionMode transmissionMode)
		{
			string pipeName = Guid.NewGuid().ToString();

			await using var server = new PipeServer(pipeName, transmissionMode: transmissionMode);
			server.MessageReceived += async (sender, e) =>
			{
				await server.SendMessageAsync(e.ClientInfo.ClientId, $"ECHO:{e.Message}");
			};
			await server.StartAsync();

			await using var client = new PipeClient(pipeName, transmissionMode: transmissionMode);
			string message = "Hello, server!" + Guid.NewGuid();
			string? receivedMessage = null;
			client.Connected += async (sender, args) =>
			{
				await client.SendMessageAsync(message);
			};
			client.MessageReceived += (sender, e) =>
			{
				receivedMessage = e.Message;
			};

			await client.ConnectAsync();

			Assert.IsTrue(await WaitUntilAsync(() => receivedMessage != null));
			Assert.AreEqual("ECHO:" + message, receivedMessage);
		}

		[TestMethod]
		public async Task SupportClientName()
		{
			string pipeName = Guid.NewGuid().ToString();
			string clientName = "MyClient";
			string? receivedClientName = null;

			await using var server = new PipeServer(pipeName, enableClientName: true);
			server.MessageReceived += async (sender, e) =>
			{
				receivedClientName = e.ClientInfo.ClientName;
				await server.SendMessageAsync(e.ClientInfo.ClientId, $"ECHO:{e.Message}");
			};
			await server.StartAsync();

			await using var client = new PipeClient(pipeName, clientName: clientName);
			string message = "Hello, server!" + Guid.NewGuid();
			string? receivedMessage = null;

			client.Connected += async (sender, args) =>
			{
				await client.SendMessageAsync(message);
			};
			client.MessageReceived += (sender, e) =>
			{
				receivedMessage = e.Message;
			};

			await client.ConnectAsync();

			Assert.IsTrue(await WaitUntilAsync(() => receivedMessage != null));
			Assert.AreEqual("ECHO:" + message, receivedMessage);
			Assert.AreEqual(clientName, receivedClientName);
		}

		[TestMethod]
		public async Task ConnectAsync_WhenServerMissing_ShouldThrow()
		{
			await using var client = new PipeClient(Guid.NewGuid().ToString());

			await Assert.ThrowsExceptionAsync<TimeoutException>(() => client.ConnectAsync(100));
		}

		[TestMethod]
		public async Task TryConnectAsync_WhenServerMissing_ShouldReturnFalse()
		{
			await using var client = new PipeClient(Guid.NewGuid().ToString());

			Assert.IsFalse(await client.TryConnectAsync(100));
		}

		[TestMethod]
		public async Task SupportAutoReconnect()
		{
			string pipeName = Guid.NewGuid().ToString();
			string clientName = "MyClient";
			string? receivedClientName = null;
			var disconnectedCount = 0;
			var connectedCount = 0;

			await using var server = new PipeServer(pipeName, enableClientName: true);
			server.MessageReceived += async (sender, e) =>
			{
				receivedClientName = e.ClientInfo.ClientName;
				await server.SendMessageAsync(e.ClientInfo.ClientId, $"ECHO:{e.Message}");
			};

			await using var client = new PipeClient(pipeName, clientName: clientName);
			string message = "Hello, server!" + Guid.NewGuid();
			string? receivedMessage = null;

			client.Connected += async (sender, args) =>
			{
				Interlocked.Increment(ref connectedCount);
				await client.SendMessageAsync(message);
			};
			client.Disconnected += (sender, args) => Interlocked.Increment(ref disconnectedCount);
			client.MessageReceived += (sender, e) =>
			{
				receivedMessage = e.Message;
			};

			client.StartConnectWithAutoReconnection();
			await Task.Delay(100);
			Assert.IsFalse(client.IsConnected, "客户端先启动，应该无法连接。");

			await server.StartAsync();
			Assert.IsTrue(await WaitUntilAsync(() => client.IsConnected), "服务器启动后，应该可以连接。");
			Assert.IsTrue(await WaitUntilAsync(() => receivedMessage != null), "客户端应该收到回显。");
			Assert.AreEqual(clientName, receivedClientName);

			await server.StopAsync();
			Assert.IsTrue(await WaitUntilAsync(() => !client.IsConnected && disconnectedCount > 0), "服务器停止后，应该断开连接并触发事件。");

			receivedMessage = null;
			await server.StartAsync();
			Assert.IsTrue(await WaitUntilAsync(() => client.IsConnected && connectedCount >= 2, 4000), "服务器重启后，应该重新连接。");

			await client.StopConnectWithAutoReconnectionAsync();
		}

		[TestMethod]
		public async Task AutoReconnect_WhenDisposed_ShouldStopWithoutDelay()
		{
			var client = new PipeClient(Guid.NewGuid().ToString());
			client.StartConnectWithAutoReconnection();

			var watch = Stopwatch.StartNew();
			await client.DisposeAsync();
			watch.Stop();

			Assert.IsTrue(watch.Elapsed < TimeSpan.FromSeconds(1), "停止自动重连不应等待完整重试间隔。");
		}

		[TestMethod]
		public async Task AutoReconnect_WhenDisconnectCalledConcurrently_ShouldNotRace()
		{
			string pipeName = Guid.NewGuid().ToString();
			await using var server = new PipeServer(pipeName);
			await server.StartAsync();

			await using var client = new PipeClient(pipeName);
			client.StartConnectWithAutoReconnection();

			Assert.IsTrue(await WaitUntilAsync(() => client.IsConnected));

			await Task.WhenAll(
				client.DisconnectAsync(),
				client.StopConnectWithAutoReconnectionAsync());

			Assert.IsFalse(client.IsConnected);
		}

		[TestMethod]
		public async Task PipeServerDispose_ShouldReleasePipeBeforeReturn()
		{
			string pipeName = Guid.NewGuid().ToString();

			var server = new PipeServer(pipeName);
			await server.StartAsync();
			server.Dispose();

			await using var nextServer = new PipeServer(pipeName);
			await nextServer.StartAsync();

			Assert.IsTrue(nextServer.IsRunning);
		}

		[TestMethod]
		public async Task SendMessageAsync_WhenMessageEmpty_ShouldThrowArgumentException()
		{
			string pipeName = Guid.NewGuid().ToString();
			await using var server = new PipeServer(pipeName);
			await server.StartAsync();
			await using var client = new PipeClient(pipeName);
			await client.ConnectAsync();

			await Assert.ThrowsExceptionAsync<ArgumentException>(() => client.SendMessageAsync(string.Empty));
		}

		private static async Task<bool> WaitUntilAsync(Func<bool> condition, int timeoutMs = 2000)
		{
			var watch = Stopwatch.StartNew();
			while (watch.ElapsedMilliseconds < timeoutMs)
			{
				if (condition())
				{
					return true;
				}

				await Task.Delay(25);
			}

			return condition();
		}
	}
}
