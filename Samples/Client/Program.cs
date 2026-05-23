using System.Diagnostics;
using SimpleNamedPipe;

namespace PipeClientTest
{
	internal class Program
	{
		static async Task Main(string[] args)
		{
			await TestClientReconnect();

			//for (int i = 0; i < 1; i++)
			//{
			//	await TestClient(i);
			//}

			Console.WriteLine("Bye from main...");
			//Console.WriteLine("Press any key to exit...");
			//Console.ReadKey();
		}

		static async Task TestClient(int index)
		{
			var client = new PipeClient(
				"MyPipeName",
				clientName: $"Client_{index}",
				transmissionMode: MessageTransmissionMode.ByteBasedBigEndian);

			// 注册事件处理器
			client.Connected += (sender, e) =>
				Console.WriteLine($"Connected: {e.Message}");

			client.Disconnected += (sender, e) =>
				Console.WriteLine($"Disconnected: {e.Message}");

			client.Reconnecting += (s, e) => Console.WriteLine($"Reconnecting: {e.Message}");


			client.MessageReceived += (sender, e) =>
			{
				//var msg = e.Message.Length > 30 ? e.Message.Substring(0, 30) + "..." : e.Message;

				//Console.WriteLine($"Received: {e.Message.Length} {msg}...");
				if (e.Message.Length > 30)
				{
					Console.Write($"Received: {e.Message.Length} bytes");
				}
				else
				{
					Console.WriteLine($"Received: {e.Message}");
				}
			};

			client.Error += (sender, e) =>
				Console.WriteLine($"Error: {e.Message}, Exception: {e.Exception}");

			try
			{
				// 连接到服务器
				await client.ConnectAsync();
				await client.SendMessageAsync("Hello Server!");
				await client.SendMessageAsync("Hello Server2!");
				
				var watch = Stopwatch.StartNew();

				// 发送消息
				for (int i = 0; i < 10000; i++)
				{
					await client.SendMessageAsync($"Hello {i}!");
				}

				await client.SendMessageAsync(new string('z', 102400));

				Console.WriteLine($"耗时:{watch.ElapsedMilliseconds}ms");

				// 等待一段时间接收响应
				await Task.Delay(5000);

				//// 断开连接
				//await client.DisconnectAsync();

				//await client.ConnectAsync();
				//await client.SendMessageAsync("Hello Server 2!");
				//// 等待一段时间接收响应
				//await Task.Delay(2000);

				// 断开连接
				await client.DisconnectAsync();
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Operation failed: {ex.Message}");
			}
			finally
			{
				client.Dispose();
			}
		}

		static async Task TestClientReconnect()
		{
			var client = new PipeClient("MyPipeName",
				clientName: "Client1");

			// 注册事件处理器
			client.Connected += async (sender, e) =>
			{
				Console.WriteLine($"Connected: {e.Message}");

				await client.SendMessageAsync("Hello Server!");
			};

			client.Disconnected += (sender, e) =>
				Console.WriteLine($"Disconnected: {e.Message}");

			client.Reconnecting += (s, e) => Console.WriteLine($"Reconnecting: {e.Message}");


			client.MessageReceived += (sender, e) =>
			{
				//var msg = e.Message.Length > 30 ? e.Message.Substring(0, 30) + "..." : e.Message;

				//Console.WriteLine($"Received: {e.Message.Length} {msg}...");
				if (e.Message.Length > 30)
				{
					Console.Write($".");
				}
				else
				{
					Console.WriteLine($"Received: {e.Message}");
				}
			};

			client.Error += (sender, e) =>
				Console.WriteLine($"Error: {e.Message}, Exception: {e.Exception}");

			try
			{
				// 连接到服务器
				client.StartConnectWithAutoReconnection();

				
				//Console.WriteLine($"Received: {e.Message}");
				//var watch = Stopwatch.StartNew();

				//// 发送消息
				//for (int i = 0; i < 10000; i++)
				//{
				//	await client.SendMessageAsync($"Hello Server! {i}");

				//}

				//await client.SendMessageAsync(new string('z', 102400));

				//Console.WriteLine($"耗时:{watch.ElapsedMilliseconds}ms");

				// 等待一段时间接收响应
				//await Task.Delay(2000);

				//// 断开连接
				//await client.DisconnectAsync();

				//await client.ConnectAsync();
				//await client.SendMessageAsync("Hello Server 2!");
				//// 等待一段时间接收响应
				//await Task.Delay(2000);

				//// 断开连接
				//await client.DisconnectAsync();
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Operation failed: {ex.Message}");
			}
			finally
			{
				//client.Dispose();
			}

			Console.WriteLine("Press any key to exit...");
			
			

			Console.ReadKey();

			await client.DisconnectAsync();
			Console.WriteLine("bye...");

			//client.Dispose();
		}
	}
}
