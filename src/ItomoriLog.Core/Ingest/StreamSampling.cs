namespace ItomoriLog.Core.Ingest;

public static class StreamSampling
{
    public static async Task<byte[]> ReadPrefixAsync(Stream stream, int maxBytes, CancellationToken ct = default)
    {
        if (maxBytes <= 0)
            return [];

        var buffer = new byte[maxBytes];
        var totalRead = 0;
        while (totalRead < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(totalRead, buffer.Length - totalRead), ct);
            if (read == 0)
                break;

            totalRead += read;
        }

        return totalRead == buffer.Length ? buffer : buffer[..totalRead];
    }
}

internal sealed class ReplayPrefixStream : Stream
{
    private readonly byte[] _prefix;
    private readonly Stream _inner;
    private readonly bool _leaveInnerOpen;
    private int _prefixOffset;
    private bool _disposed;

    public ReplayPrefixStream(byte[] prefix, Stream inner, bool leaveInnerOpen = false)
    {
        _prefix = prefix ?? [];
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _leaveInnerOpen = leaveInnerOpen;
    }

    public override bool CanRead => !_disposed && _inner.CanRead;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
        => Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        ThrowIfDisposed();

        var totalRead = 0;
        if (_prefixOffset < _prefix.Length)
        {
            var prefixCount = Math.Min(buffer.Length, _prefix.Length - _prefixOffset);
            _prefix.AsSpan(_prefixOffset, prefixCount).CopyTo(buffer);
            _prefixOffset += prefixCount;
            totalRead += prefixCount;
            buffer = buffer[prefixCount..];
        }

        if (buffer.Length == 0)
            return totalRead;

        return totalRead + _inner.Read(buffer);
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        var totalRead = 0;
        if (_prefixOffset < _prefix.Length)
        {
            var prefixCount = Math.Min(buffer.Length, _prefix.Length - _prefixOffset);
            _prefix.AsMemory(_prefixOffset, prefixCount).CopyTo(buffer);
            _prefixOffset += prefixCount;
            totalRead += prefixCount;
            buffer = buffer[prefixCount..];
        }

        if (buffer.Length == 0)
            return totalRead;

        return totalRead + await _inner.ReadAsync(buffer, cancellationToken);
    }

    public override void Flush() => ThrowIfDisposed();

    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        return _inner.FlushAsync(cancellationToken);
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (_disposed)
            return;

        if (disposing && !_leaveInnerOpen)
            _inner.Dispose();

        _disposed = true;
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        if (!_leaveInnerOpen)
            await _inner.DisposeAsync();
        _disposed = true;
        await base.DisposeAsync();
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(ReplayPrefixStream));
    }
}
