"""输出解释器及依赖环境；仅合成 AES-GCM 数据，不接触 SDK 配置或凭据。"""

import json
import platform
import ssl
import sys


def main():
    import cryptography
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.backends.openssl.backend import backend
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    key = bytes(16)
    nonce = bytes(12)
    plaintext = bytes(16)
    expected = bytes.fromhex("0388dace60b6a392f328c2b971b2fe78ab6e47d42cec13bdf53a67b21257bddf")
    encrypted = AESGCM(key).encrypt(nonce, plaintext, b"")
    if encrypted != expected or AESGCM(key).decrypt(nonce, encrypted, b"") != plaintext:
        raise RuntimeError("AES-GCM known-answer check failed")
    try:
        AESGCM(key).decrypt(nonce, encrypted[:-1] + bytes([encrypted[-1] ^ 1]), b"")
    except InvalidTag:
        pass
    else:
        raise RuntimeError("AES-GCM authentication rejection failed")
    print(
        json.dumps(
            {
                "python": platform.python_version(),
                "implementation": platform.python_implementation(),
                "platform": platform.platform(),
                "machine": platform.machine(),
                "executable": sys.executable,
                "stdlib_tls": ssl.OPENSSL_VERSION,
                "cryptography": cryptography.__version__,
                "cryptography_openssl": backend.openssl_version_text(),
                "aes_gcm_known_answer": "pass",
                "aes_gcm_invalid_tag": "pass",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
