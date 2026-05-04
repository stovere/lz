import hashlib
import hmac
import os

BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


def base62_encode(data: bytes) -> str:
    """bytes -> base62 (0-9A-Za-z)"""
    num = int.from_bytes(data, "big")
    if num == 0:
        return BASE62[0]

    out = []
    while num > 0:
        num, r = divmod(num, 62)
        out.append(BASE62[r])
    return "".join(reversed(out))


def base62_decode(s: str) -> bytes:
    """base62 -> bytes"""
    num = 0
    for ch in s:
        num = num * 62 + BASE62.index(ch)
    # 推算所需 bytes 长度
    length = (num.bit_length() + 7) // 8
    return num.to_bytes(length, "big")


def _keystream(key_bytes: bytes, nonce: bytes, length: int) -> bytes:
    """用 SHA256(key + nonce + counter) 生成伪随机金鑰流"""
    out = b""
    counter = 0
    while len(out) < length:
        counter_bytes = counter.to_bytes(4, "big")
        out += hashlib.sha256(key_bytes + nonce + counter_bytes).digest()
        counter += 1
    return out[:length]


def encrypt(text: str, key: str) -> str:
    key_bytes = key.encode()
    data = text.encode()

    # 随机 nonce（每次不同，提高安全性）
    nonce = os.urandom(6)  # 6 bytes 足够短，也不会超过你 62 字限制

    # keystream
    ks = _keystream(key_bytes, nonce, len(data))

    # XOR 混淆
    mixed = bytes(b ^ k for b, k in zip(data, ks))

    # HMAC 前 6 bytes（防篡改）
    mac = hmac.new(key_bytes, nonce + mixed, hashlib.sha256).digest()[:6]

    # 打包：nonce + ciphertext + mac
    token_bytes = nonce + mixed + mac

    # 最终：只用 0-9A-Za-z
    return base62_encode(token_bytes)


def decrypt(token: str, key: str) -> str:
    key_bytes = key.encode()

    raw = base62_decode(token)

    if len(raw) < 6 + 6:
        raise ValueError("token 太短")

    nonce = raw[:6]
    mac_given = raw[-6:]
    mixed = raw[6:-6]

    # HMAC 校验
    mac_calc = hmac.new(key_bytes, nonce + mixed, hashlib.sha256).digest()[:6]
    if not hmac.compare_digest(mac_given, mac_calc):
        raise ValueError("MAC 校验失败，token 可能被篡改")

    # 解密 XOR
    ks = _keystream(key_bytes, nonce, len(mixed))
    data = bytes(b ^ k for b, k in zip(mixed, ks))

    return data.decode()


if __name__ == "__main__":
    plaintext = "12345678901234566;399;YbfowW8h2d"
    key = "LyAIXES"

    encrypted = encrypt(plaintext, key)

    encrypted = "6zU51Bt3W92gbdhGyWVGJ1yJVdCGGO3jD7XnRMDnf3iL2V"
    decrypted = decrypt(encrypted, key)

    print("加密 =", encrypted)
    print("长度 =", len(encrypted))
    print("解密 =", decrypted)
