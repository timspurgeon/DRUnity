
using System;
using System.Collections.Generic;
using Server.Common;

namespace Server.Net {
  public class PacketRouter {
    readonly Dictionary<Op, Func<byte[], object, byte[]?>> handlers = new();
    public void On(Op op, Func<byte[], object, byte[]?> fn) => handlers[op] = fn;
    public byte[]? Dispatch(Op op, byte[] payload, object ctx) =>
      handlers.TryGetValue(op, out var fn) ? fn(payload, ctx) : null;
  }
}
