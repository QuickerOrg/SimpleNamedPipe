using System;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

#if !NETFRAMEWORK
using System.Buffers;
using System.Buffers.Binary;
#endif

namespace SimpleNamedPipe;

/// <summary>
/// BinaryFormatter兼容的编码器，用于与旧版本BinaryFormatter序列化的字符串格式保持兼容
/// 仅支持字符串类型的序列化和反序列化
/// 
/// 使用示例：
/// <code>
/// // 创建服务器，使用BinaryFormatter兼容的小端字节序编码器
/// var server = new PipeServer("MyPipe", transmissionMode: MessageTransmissionMode.BinaryFormatterCompatibleLittleEndian);
/// 
/// // 创建客户端，使用BinaryFormatter兼容的大端字节序编码器
/// var client = new PipeClient("MyPipe", transmissionMode: MessageTransmissionMode.BinaryFormatterCompatibleBigEndian);
/// </code>
/// </summary>
internal class BinaryFormatterCompatibleEncoder : IMessageEncoder
{
    public PipeTransmissionMode TransmissionMode => PipeTransmissionMode.Byte;

#if !NETFRAMEWORK
    private readonly ArrayPool<byte> _arrayPool = ArrayPool<byte>.Shared;
#endif
    private static readonly UTF8Encoding Utf8Encoding = new(false);
    private readonly bool _isLittleEndian;
    private readonly int _maxMessageBytes;

    // BinaryFormatter 序列化头
    private static readonly byte[] BINARY_FORMATTER_HEADER = {
        0x00, // RecordTypeEnum.SerializedStreamHeader
        0x01, 0x00, 0x00, 0x00, // RootId = 1
        0xFF, 0xFF, 0xFF, 0xFF, // HeaderId = -1
        0x01, 0x00, 0x00, 0x00, // MajorVersion = 1
        0x00, 0x00, 0x00, 0x00  // MinorVersion = 0
    };

    /// <summary>
    /// 初始化BinaryFormatterCompatibleEncoder
    /// </summary>
    /// <param name="isLittleEndian">是否使用小端字节序，true为小端，false为大端</param>
    /// <param name="maxMessageBytes">允许接收的最大消息字节数。</param>
    public BinaryFormatterCompatibleEncoder(bool isLittleEndian = true, int maxMessageBytes = 10 * 1024 * 1024)
    {
        _isLittleEndian = isLittleEndian;
        _maxMessageBytes = maxMessageBytes > 0
            ? maxMessageBytes
            : throw new ArgumentOutOfRangeException(nameof(maxMessageBytes));
    }

    public async Task WriteMessageAsync(PipeStream stream, string message, CancellationToken cancellationToken)
    {
        if (message == null)
            message = string.Empty;

        byte[] payloadBytes;
        using (var ms = new MemoryStream())
        {
            // Use BinaryWriter to correctly format the string with a 7-bit encoded length
            using (var writer = new BinaryWriter(ms, Utf8Encoding, true))
            {
                // 1. SerializationHeaderRecord
                writer.Write(BINARY_FORMATTER_HEADER);

                // 2. BinaryObjectString record
                writer.Write((byte)0x06); // RecordTypeEnum.BinaryObjectString
                writer.Write(1);         // ObjectId = 1 (same as RootId)

                // 2c. String value (BinaryWriter.Write(string) handles 7-bit encoded length)
                writer.Write(message);

                // 3. MessageEnd record
                writer.Write((byte)0x0B); // RecordTypeEnum.MessageEnd
            }
            payloadBytes = ms.ToArray();
        }

        int totalDataLength = payloadBytes.Length;
        int bufferLen = sizeof(int) + totalDataLength;

#if NETFRAMEWORK
        byte[] buffer = new byte[bufferLen];
        if (_isLittleEndian)
        {
            WriteInt32LittleEndian(buffer, totalDataLength);
        }
        else
        {
            WriteInt32BigEndian(buffer, totalDataLength);
        }
        Buffer.BlockCopy(payloadBytes, 0, buffer, sizeof(int), totalDataLength);
        await stream.WriteAsync(buffer, 0, bufferLen, cancellationToken);
        await stream.FlushAsync(cancellationToken);
#else
        byte[] buffer = _arrayPool.Rent(bufferLen);
        try
        {
            var span = buffer.AsSpan();
            if (_isLittleEndian)
            {
                BinaryPrimitives.WriteInt32LittleEndian(span, totalDataLength);
            }
            else
            {
                BinaryPrimitives.WriteInt32BigEndian(span, totalDataLength);
            }
            payloadBytes.CopyTo(span.Slice(sizeof(int)));
            
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
        // Read the length of the entire payload
        byte[] lengthBuffer = new byte[sizeof(int)];
        await ReadExactAsync(stream, lengthBuffer, sizeof(int), cancellationToken);

#if NETFRAMEWORK
        var dataLength = _isLittleEndian ? 
            ReadInt32LittleEndian(lengthBuffer) : 
            ReadInt32BigEndian(lengthBuffer);
#else
        var dataLength = _isLittleEndian ? 
            BinaryPrimitives.ReadInt32LittleEndian(lengthBuffer) :
            BinaryPrimitives.ReadInt32BigEndian(lengthBuffer);
#endif

        if (dataLength <= 0 || dataLength > _maxMessageBytes)
        {
            throw new InvalidDataException($"Invalid data length: {dataLength}");
        }

#if NETFRAMEWORK
        var dataBuffer = new byte[dataLength];
        await ReadExactAsync(stream, dataBuffer, dataLength, cancellationToken);
        return ParseBinaryFormatterString(dataBuffer);
#else
        var dataBuffer = _arrayPool.Rent(dataLength);
        try
        {
            await ReadExactAsync(stream, dataBuffer, dataLength, cancellationToken);
            // Use AsSpan to avoid copying the rented buffer.
            return ParseBinaryFormatterString(dataBuffer.AsSpan(0, dataLength));
        }
        finally
        {
            _arrayPool.Return(dataBuffer);
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
    private string ParseBinaryFormatterString(byte[] data)
    {
        using (var ms = new MemoryStream(data))
        using (var reader = new BinaryReader(ms, Utf8Encoding, true))
        {
            // 1. Verify header
            var headerBytes = reader.ReadBytes(BINARY_FORMATTER_HEADER.Length);
            if (headerBytes.Length < BINARY_FORMATTER_HEADER.Length || !headerBytes.SequenceEqual(BINARY_FORMATTER_HEADER))
            {
                throw new InvalidDataException("Invalid BinaryFormatter header.");
            }

            // 2. Verify BinaryObjectString record
            if (reader.ReadByte() != 0x06) // RecordTypeEnum.BinaryObjectString
            {
                throw new InvalidDataException("Expected BinaryObjectString record.");
            }
            reader.ReadInt32(); // Skip ObjectId

            // 2c. Read string value
            string result = reader.ReadString();

            // 3. Verify MessageEnd
            if (reader.BaseStream.Position < reader.BaseStream.Length && reader.ReadByte() != 0x0B) // RecordTypeEnum.MessageEnd
            {
                throw new InvalidDataException("Expected MessageEnd record.");
            }
            
            return result;
        }
    }
#else
    private string ParseBinaryFormatterString(ReadOnlySpan<byte> data)
    {
        int offset = 0;

        // 1. Verify header
        if (data.Length < offset + BINARY_FORMATTER_HEADER.Length || 
            !data.Slice(offset, BINARY_FORMATTER_HEADER.Length).SequenceEqual(BINARY_FORMATTER_HEADER))
        {
            throw new InvalidDataException("Invalid BinaryFormatter header.");
        }
        offset += BINARY_FORMATTER_HEADER.Length;

        // 2. Verify BinaryObjectString record
        if (data[offset++] != 0x06) // RecordTypeEnum.BinaryObjectString
        {
            throw new InvalidDataException("Expected BinaryObjectString record.");
        }
        
        // Skip ObjectId
        offset += 4; 

        // 2c. Read string (manually, since there is no BinaryReader for Span)
        int stringLength = Read7BitEncodedInt(data, ref offset);
        if (stringLength < 0) throw new IOException("Invalid string length in data stream.");
        
        if (offset + stringLength > data.Length)
        {
            throw new IOException("Unexpected end of stream while reading string.");
        }
        
        string resultString = Utf8Encoding.GetString(data.Slice(offset, stringLength));
        offset += stringLength;

        // 3. Verify MessageEnd
        if (offset < data.Length && data[offset] != 0x0B) // RecordTypeEnum.MessageEnd
        {
            throw new InvalidDataException("Expected MessageEnd record.");
        }

        return resultString;
    }

    private static int Read7BitEncodedInt(ReadOnlySpan<byte> data, ref int offset)
    {
        int count = 0;
        int shift = 0;
        byte b;
        do
        {
            if (shift == 35) throw new FormatException("Invalid 7-bit encoded int format.");
            if (offset >= data.Length) throw new IOException("Unexpected end of stream while reading 7-bit encoded int.");
            
            b = data[offset++];
            count |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return count;
    }
#endif

#if NETFRAMEWORK
    static int ReadInt32LittleEndian(byte[] buffer)
    {
        return buffer[0] | (buffer[1] << 8) | (buffer[2] << 16) | (buffer[3] << 24);
    }

    static int ReadInt32BigEndian(byte[] buffer)
    {
        return (buffer[0] << 24) | (buffer[1] << 16) | (buffer[2] << 8) | buffer[3];
    }

    static void WriteInt32LittleEndian(byte[] buffer, int value)
    {
        buffer[0] = (byte)(value);
        buffer[1] = (byte)(value >> 8);
        buffer[2] = (byte)(value >> 16);
        buffer[3] = (byte)(value >> 24);
    }
    
    static void WriteInt32BigEndian(byte[] buffer, int value)
    {
        buffer[0] = (byte)(value >> 24);
        buffer[1] = (byte)(value >> 16);
        buffer[2] = (byte)(value >> 8);
        buffer[3] = (byte)(value);
    }
#endif
} 
