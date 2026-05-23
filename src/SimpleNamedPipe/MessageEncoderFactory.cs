using System;

namespace SimpleNamedPipe;

/// <summary>
/// 消息编码器工厂类
/// </summary>
public static class MessageEncoderFactory
{
    /// <summary>
    /// 根据传输模式创建对应的消息编码器
    /// </summary>
    /// <param name="transmissionMode">传输模式</param>
    /// <returns>消息编码器实例</returns>
    /// <exception cref="PlatformNotSupportedException">当在非Windows平台使用MessageBased模式时抛出</exception>
    /// <exception cref="ArgumentOutOfRangeException">当传输模式无效时抛出</exception>
    public static IMessageEncoder CreateEncoder(MessageTransmissionMode transmissionMode, int maxMessageBytes = 1024 * 1024)
    {
        if (transmissionMode == MessageTransmissionMode.MessageBased && Environment.OSVersion.Platform != PlatformID.Win32NT)
        {
            throw new PlatformNotSupportedException("MessageBasedEncoder (PipeTransmissionMode.Message) is only supported on Windows.");
        }

        if (maxMessageBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxMessageBytes), maxMessageBytes, "Message size limit must be greater than 0.");
        }

        return transmissionMode switch
        {
            MessageTransmissionMode.MessageBased => new MessageBasedEncoder(maxMessageBytes),
            MessageTransmissionMode.ByteBasedLittleEndian => new ByteBasedEncoder(isLittleEndian: true, maxMessageBytes: maxMessageBytes),
            MessageTransmissionMode.ByteBasedBigEndian => new ByteBasedEncoder(isLittleEndian: false, maxMessageBytes: maxMessageBytes),
            MessageTransmissionMode.BinaryFormatterCompatibleLittleEndian => new BinaryFormatterCompatibleEncoder(isLittleEndian: true, maxMessageBytes: maxMessageBytes),
            MessageTransmissionMode.BinaryFormatterCompatibleBigEndian => new BinaryFormatterCompatibleEncoder(isLittleEndian: false, maxMessageBytes: maxMessageBytes),
            _ => throw new ArgumentOutOfRangeException(nameof(transmissionMode), transmissionMode, "Invalid transmission mode")
        };
    }
} 
