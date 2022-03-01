import cv2
from pyzbar.pyzbar import decode

capture = cv2.VideoCapture(0)

while True:
    _, frame = capture.read()
    decode_data = decode(frame)
    try:
        print(decode_data[0][0])

    except:
        pass

    cv2.imshow('QR Code Scanner', frame)
    key = cv2.waitKey(1)
    if key == ord('q'):
        break
