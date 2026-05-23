

using SimpleNamedPipe;

namespace NamedPipeLite
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
			var server = new PipeServer(
				"MyPipeName",
				enableClientName: true,
				transmissionMode: MessageTransmissionMode.ByteBasedBigEndian);

			// 注册事件处理
			server.ClientConnected += (sender, e) =>
				Console.WriteLine($"Client {e.ClientInfo.ClientId} connected");

			server.ClientDisconnected += (sender, e) =>
				Console.WriteLine($"Client {e.ClientInfo.ClientId} disconnected");

			server.MessageReceived += async (sender, e) =>
			{
				var msg = e.Message.Length > 20 ? e.Message.Substring(0, 20) + "..." : e.Message;

				Console.WriteLine($"Received from client {e.ClientInfo.ClientId}|{e.ClientInfo.ClientName}: {e.Message.Length} {msg}...");
				// 可以在这里处理接收到的JSON消息
				try
				{
					// 示例：发送响应
					await server.SendMessageAsync(e.ClientInfo.ClientId, $"Echo: {e.Message}");

				}
				catch (Exception ex)
				{
					Console.WriteLine("发送出错：" + ex.Message);
				}
			};




			// 启动服务器
			await server.StartAsync();

			// 等待用户输入来停止服务器
			Console.WriteLine("Press Enter to stop server");
			Console.ReadLine();
			// 停止服务器
			await server.StopAsync();

			Console.WriteLine("Stopped. Press Enter to start server");
			Console.ReadLine();
			
			await server.StartAsync();
			Console.WriteLine("Started. Press Enter to stop server");
			
			Console.ReadLine();
			await server.StopAsync();

			Console.WriteLine("Stoped. Press any key to Exit...");
			Console.ReadKey();
		}
    }
}
