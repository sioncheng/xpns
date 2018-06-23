命令序列化

2 bytes magic           0xab12
8 bytes(int64) serialnumber    0x01
1 byte  command type   0x00 heartbeat 0x01 request 0x01 response
1 byte  serialization type   0x01 json
4 bytes(int32) payload length
x bytes payload

