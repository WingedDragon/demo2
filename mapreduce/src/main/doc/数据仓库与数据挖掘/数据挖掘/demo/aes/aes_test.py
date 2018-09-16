
# -*- coding:utf-8 -*-
import base64
from Crypto.Cipher import AES
from Crypto import Random
from binascii import b2a_hex, a2b_hex



def encrypt(key, data):
    # key=key, mode=AES.MODE_CBC
    cryptor = AES.new(key, AES.MODE_CBC, key)


    # 这里密钥key 长度必须为16（AES-128）、24（AES-192）、或32（AES-256）Bytes 长度.目前AES-128足够用
    length = 16
    count = len(data)
    add = length - (count % length)
    text = data + ('\0' * add)
    encrypttext = cryptor.encrypt(text)
    print(encrypttext)

    # 因为AES加密时候得到的字符串不一定是ascii字符集的，输出到终端或者保存时候可能存在问题
    # 所以这里统一把加密后的字符串转化为16进制字符串
    return b2a_hex(encrypttext)


#解密后，去掉补足的空格用strip() 去掉
def decrypt(key, text):
    cryptor = AES.new(key, AES.MODE_CBC, key)
    plain_text = cryptor.decrypt(a2b_hex(text))
    return str(plain_text,encoding='utf-8').rstrip('\0')




if __name__ == '__main__':
    data = 'aabbcc'
    aes_key = 'finupfinupfinup0'
    print(len(aes_key))

    #加密
    encrypt_data = encrypt(aes_key, data)
    print('encrypt_data={0}'.format(encrypt_data))

    #解密
    #decrypt_data = decrypt(aes_key, encrypt_data)
    #@print('decrypt_data={0}'.format(decrypt_data))
