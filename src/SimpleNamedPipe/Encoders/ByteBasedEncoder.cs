using System;
using System.IO;
using System.IO.Pipes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

#if !NETFRAMEWORK
using System.Buffers;
using System.Buffers.Binary;
#endif

namespace SimpleNamedPipe;

internal class ByteBasedEncoder : IMessageEncoder
{
	public PipeTransmissionMode TransmissionMode => PipeTransmissionMode.Byte;

#if !NETFRAMEWORK
	private readonly ArrayPool<byte> _arrayPool = ArrayPool<byte>.Shared;
#endif
	private static readonly UTF8Encoding Utf8Encoding = new(false);
	private readonly bool _isLittleEndian;
	private readonly int _maxMessageBytes;

	/// <summary>
	/// 初始化ByteBasedEncoder
	/// </summary>
	/// <param name="isLittleEndian">是否使用小端字节序，true为小端，false为大端</param>
	/// <param name="maxMessageBytes">允许接收的最大消息字节数。</param>
	public ByteBasedEncoder(bool isLittleEndian = true, int maxMessageBytes = 1024 * 1024)
	{
		_isLittleEndian = isLittleEndian;
		_maxMessageBytes = maxMessageBytes > 0
			? maxMessageBytes
			: throw new ArgumentOutOfRangeException(nameof(maxMessageBytes));
	}

	public async Task WriteMessageAsync(PipeStream stream, string message, CancellationToken cancellationToken)
	{
		//
		// 计算消息的UTF8字节长度
		int byteCount = Utf8Encoding.GetByteCount(message);
		int bufferLen = sizeof(int) + byteCount;

#if NETFRAMEWORK
		byte[] buffer = new byte[bufferLen];
		// 写入消息长度
		if (_isLittleEndian)
		{
			WriteInt32LittleEndian(buffer, byteCount);
		}
		else
		{
			WriteInt32BigEndian(buffer, byteCount);
		}
		// 直接编码到缓冲区
		Utf8Encoding.GetBytes(message, 0, message.Length, buffer, sizeof(int));
		await stream.WriteAsync(buffer, 0, bufferLen, cancellationToken);
		await stream.FlushAsync(cancellationToken);
#else
		// 从池中租用足够大的缓冲区
		byte[] buffer = _arrayPool.Rent(bufferLen);
		// 写入消息长度
		if (_isLittleEndian)
		{
			BinaryPrimitives.WriteInt32LittleEndian(buffer, byteCount);
		}
		else
		{
			BinaryPrimitives.WriteInt32BigEndian(buffer, byteCount);
		}
		try
		{
			// 直接编码到缓冲区
			Utf8Encoding.GetBytes(message, 0, message.Length, buffer, sizeof(int));
			await stream.WriteAsync(buffer, 0, bufferLen, cancellationToken);
			await stream.FlushAsync(cancellationToken);
		}
		finally
		{
			_arrayPool.Return(buffer);

		}
#endif
	}

	public async Task<string> ReadMessageAsync(PipeStream stream, CancellationToken cancellationToken)
	{
		// 读取消息长度
		byte[] lengthBuffer = new byte[sizeof(int)];

		//var read = await stream.ReadAsync(lengthBuffer, cancellationToken);
		await ReadExactAsync(stream, lengthBuffer, sizeof(int), cancellationToken);

#if NETFRAMEWORK
		var messageLength = _isLittleEndian ? 
			ReadInt32LittleEndian(lengthBuffer) : 
			ReadInt32BigEndian(lengthBuffer);
		if (messageLength <= 0)
		{
			throw new InvalidDataException($"Invalid message length: {messageLength}");
		}
		else if (messageLength > _maxMessageBytes)
		{
			throw new InvalidDataException($"Message length too long: {messageLength}");
		}

		// 从共享池租用缓冲区
		var buffer = new byte[messageLength];
		await ReadExactAsync(stream, buffer, messageLength, cancellationToken);

		// 直接解码指定长度
		return Utf8Encoding.GetString(buffer, 0, messageLength);
#else

		var messageLength = _isLittleEndian ? 
			BinaryPrimitives.ReadInt32LittleEndian(lengthBuffer) :
			BinaryPrimitives.ReadInt32BigEndian(lengthBuffer);
		if (messageLength <= 0)
		{
			throw new InvalidDataException($"Invalid message length: {messageLength}");
		}
		else if (messageLength > _maxMessageBytes)
		{
			throw new InvalidDataException($"Message length too long: {messageLength}");
		}

		// 从共享池租用缓冲区
		var buffer = _arrayPool.Rent(messageLength);
		try
		{
			await ReadExactAsync(stream, buffer, messageLength, cancellationToken);

			// 直接解码指定长度
			return Utf8Encoding.GetString(buffer, 0, messageLength);
		}
		finally
		{
			// 归还缓冲区到池
			_arrayPool.Return(buffer);
		}
#endif
	}

	private static async Task ReadExactAsync(PipeStream stream, byte[] buffer, int count, CancellationToken cancellationToken)
	{
		var offset = 0;
		while (offset < count)
		{
			var read = await stream.ReadAsync(buffer, offset, count - offset, cancellationToken);
			if (read == 0)
			{
				throw new IOException("Connection closed while reading message");
			}

			offset += read;
		}
	}

#if NETFRAMEWORK
	static int ReadInt32LittleEndian(byte[] buffer)
	{
		if (buffer == null || buffer.Length < 4)
			throw new ArgumentException("Buffer must have at least 4 bytes.");

		return buffer[0]
		       | (buffer[1] << 8)
		       | (buffer[2] << 16)
		       | (buffer[3] << 24);
	}

	static int ReadInt32BigEndian(byte[] buffer)
	{
		if (buffer == null || buffer.Length < 4)
			throw new ArgumentException("Buffer must have at least 4 bytes.");

		return (buffer[0] << 24)
		       | (buffer[1] << 16)
		       | (buffer[2] << 8)
		       | buffer[3];
	}

	static void WriteInt32LittleEndian(byte[] buffer, int value)
	{
		if (buffer == null || buffer.Length < 4)
			throw new ArgumentException("Buffer must have at least 4 bytes.");

		buffer[0] = (byte)(value & 0xFF);
		buffer[1] = (byte)((value >> 8) & 0xFF);
		buffer[2] = (byte)((value >> 16) & 0xFF);
		buffer[3] = (byte)((value >> 24) & 0xFF);
	}

	static void WriteInt32BigEndian(byte[] buffer, int value)
	{
		if (buffer == null || buffer.Length < 4)
			throw new ArgumentException("Buffer must have at least 4 bytes.");

		buffer[0] = (byte)((value >> 24) & 0xFF);
		buffer[1] = (byte)((value >> 16) & 0xFF);
		buffer[2] = (byte)((value >> 8) & 0xFF);
		buffer[3] = (byte)(value & 0xFF);
	}
#endif

}
