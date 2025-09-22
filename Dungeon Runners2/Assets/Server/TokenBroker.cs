using System;
using System.Security.Cryptography;

namespace Server.Common
{
    public static class TokenBroker
    {
        // Issues a non-zero 32-bit token.
        public static uint Issue(int accountId)
        {
            Span<byte> b = stackalloc byte[4];
            RandomNumberGenerator.Fill(b);
            uint t = (uint)(b[0] | (b[1] << 8) | (b[2] << 16) | (b[3] << 24));
            if (t == 0) t = 0xA1B2C3D4;
            return t;
        }
    }
}
