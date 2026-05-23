using System;
using System.Buffers;
using System.IO;
using System.IO.Pipes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleNamedPipe;

/// <summary>
/// 基于 PipeTransmissionMode.Message 的消息编码器，直接发送 UTF-8 字符串。
/// </summary>
internal class MessageBasedEncoder : IMessageEncoder
{
	private readonly int _maxMessageBytes;

	public MessageBasedEncoder(int maxMessageBytes = 1024 * 1024)
	{
		_maxMessageBytes = maxMessageBytes > 0
			? maxMessageBytes
			: throw new ArgumentOutOfRangeException(nameof(maxMessageBytes));
	}

#pragma warning disable CA1416
	public PipeTransmissionMode TransmissionMode => PipeTransmissionMode.Message;
#pragma warning restore CA1416

	public async Task WriteMessageAsync(PipeStream stream, string message, CancellationToken cancellationToken)
	{
		byte[] buffer = Encoding.UTF8.GetBytes(message);
		await stream.WriteAsync(buffer, 0, buffer.Length, cancellationToken).ConfigureAwait(false);
		await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
	}

	public async Task<string> ReadMessageAsync(PipeStream stream, CancellationToken cancellationToken)
	{
		byte[] buffer = ArrayPool<byte>.Shared.Rent(4096);
		var messageBuilder = new StringBuilder();
		var totalBytesRead = 0;

		try
		{
			do
			{
				int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, cancellationToken).ConfigureAwait(false);
				if (bytesRead == 0)
				{
					break;
				}

				totalBytesRead += bytesRead;
				if (totalBytesRead > _maxMessageBytes)
				{
					throw new InvalidDataException($"Message length too long: {totalBytesRead}");
				}

				messageBuilder.Append(Encoding.UTF8.GetString(buffer, 0, bytesRead));
			}
			while (!stream.IsMessageComplete);
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(buffer);
		}

		return messageBuilder.ToString();
	}
}
