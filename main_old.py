import serial
import psycopg2
import os
import numpy as np
import time
import setproctitle
import binascii

DBPASS = "inserter"
DBUSER = "inserter"

PACKET_OFFSET = 6


def connect_db():
    host = "host='192.168.1.68'"
    db = "dbname='datacollection'"
    identity = "user='"+DBUSER+"' password='"+DBPASS+"'"
    conn = psycopg2.connect(host+" "+db+" "+identity)
    return conn


def disconnect_db(conn):
    conn.close()


def insert(cur, values, plan):
    cur.execute("execute "+plan+" (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", values)


def parse_cell_voltages(packet):
    # Reads cell voltages from packet
    # Each Cell voltage is 16 bits
    if len(packet) < 34:
        return
    num_of_cells = 16
    start_offset = PACKET_OFFSET

    result = {}
    total_voltage = 0.0
    cell = 0
    for loc in range(start_offset, num_of_cells * 2 + start_offset, 2):
        volts = float(packet[loc] * 256 + packet[loc+1])/1000
        print(f"Cell:{cell} voltage:{volts}V")
        result[cell] = volts
        total_voltage += volts
        cell += 1
    result['total'] = total_voltage
    print(f"Total voltage:{round(total_voltage, 1)}V")
    return result

def parse_cell_stats(packet):
    cell_high_index = packet[115]
    cell_high = read16float(packet, 110+PACKET_OFFSET)

    cell_low_index = packet[118]
    cell_low = read16float(packet, 113+PACKET_OFFSET)
    

    cell_avg = read16float(packet, 115+PACKET_OFFSET)

    print(f"high:{cell_high} ({cell_high_index})")
    print(f"low:{cell_low} ({cell_low_index})")
    print(f"avg:{cell_avg}")


def parse_temperature(packet):
    res = ""
    for x in packet[85:97]:
        res += "{:02x}".format(x,2)
    print(res)
    
    mosfet = packet[85+PACKET_OFFSET] * 256 + packet[86+PACKET_OFFSET]
    balance = packet[87+PACKET_OFFSET] * 256 + packet[88+PACKET_OFFSET]
    temp1 = packet[89+PACKET_OFFSET] * 256 + packet[90+PACKET_OFFSET]
    temp2 = packet[91+PACKET_OFFSET] * 256 + packet[92+PACKET_OFFSET]
    temp3 = packet[93+PACKET_OFFSET] * 256 + packet[94+PACKET_OFFSET]
    temp4 = packet[95+PACKET_OFFSET] * 256 + packet[96+PACKET_OFFSET]
    print(f"temp1:{temp1} temp2:{temp2} temp3:{temp3} temp4:{temp4} balance:{balance} mosfet:{mosfet}")
    return {'temp1':temp1, 'temp2': temp2, 'balance':balance, 'mosfet':mosfet}

def parse_capacity(packet):
    capacity = read32int(packet, 73+PACKET_OFFSET)
    print(f"capacity:{round(capacity/1000000, 3)}Ah")
    return round(capacity/1000000, 3)
 
def parse_power(packet):
    power = read32int(packet, 105+PACKET_OFFSET)
    print(f"power: {power}W")
    return power

def parse_state_of_charge(packet):
    print(f"SOC:{packet[68+PACKET_OFFSET]}%")
    return int(packet[68+PACKET_OFFSET])

def parse_current(packet):
    rawc = read32int(packet, 64+PACKET_OFFSET)
    current = round(float(read32int(packet, 64+PACKET_OFFSET)) * 0.1, 1)
    res = ""
    #for x in response[70:80]:
    #    res += "{:02x}".format(x,2)
    #print(res)
    print(f"rawc:{rawc}, Current:{current}A")
    return current

def read32int(packet, offset):
    sign_conv = 0
    # If 32bit value first bits are high then this is negative number
    if packet[offset] > 0xF0:
        sign_conv = 0XFF
    res = (packet[offset]-sign_conv) *  0xFFFFFF
    res += (packet[offset+1]-sign_conv) * 0x00FFFF
    res += (packet[offset+2]-sign_conv) * 0x0000FF
    res += (packet[offset+3]-sign_conv) 
    return res

def read16float(packet, offset):
    res = float(packet[offset]*256 + packet[offset+1])
    return res

#ff db db 00 00 00 00 

if __name__ == "__main__":
    print("Main starts")
    setproctitle.setproctitle('bmscollector')

    ser = serial.Serial('/dev/ttyUSB0', 115200, timeout=5)


    query = [0xDB, 0xDB, 0x00, 0x00, 0x00, 0x00]
    keepalive = [0x5a, 0x5a, 0xff, 0x00, 0x00, 0xff]
    #query = "dbdb00000000"
    conn = connect_db()

    cur = conn.cursor()
    sqlquery = """ prepare insertplan as
                 INSERT INTO
                  "BMS_RAW"
                 VALUES
                  ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, current_timestamp) """
    cur.execute(sqlquery)


    every_n = 0
    #fh = open('db3.txt', 'w')
    lastunk = 0
    while(True):
        #print("Sending next request")
        #every_n += 1
        #if every_n == 5:
            #
            #every_n = 0
        ser.write(serial.to_bytes(query))
        response = list(ser.read(size=140))

        print("resplen:"+str(len(response)))
        res = ""
        
        for x in response:
            res += "{:02x}".format(x,2)
        print(res)
        #fh.write(res+"\n")
        #fh.flush()


        checksum = 0
        for x in response[:-2]:
            checksum += x
        checksum -= 696
        print("Checksum:"+str(checksum % 0xFFFF))
        print("Checksum:{:02x}".format((checksum % 0xFFFF),2))

        unk = response[-4] * 0xFF + response[-3]
        packetchecksum = response[-2] * 0xFF + response[-1]
        print("unk:"+str(unk))
        print("lastunk:"+str(lastunk))
        print("Lastunkdelta:"+str(unk - lastunk))
        lastunk = unk
        print("packetchecksum:"+str(packetchecksum))
        print("Delta:"+str((checksum % 0xFFFF) - packetchecksum))

        if len(response) == 140 and response[0:3] == [0xaa, 0x55, 0xaa]:
            PACKET_OFFSET = 6
            volts = parse_cell_voltages(response)
            temps = parse_temperature(response)
            capacity = parse_capacity(response)
            power = parse_power(response)
            parse_cell_stats(response)
            soc = parse_state_of_charge(response)
            current = parse_current(response)

            values = (volts[0],
                      volts[1],
                      volts[2],
                      volts[3],
                      volts[4],
                      volts[5],
                      volts[6],
                      volts[7],
                      volts[8],
                      volts[9],
                      volts[10],
                      volts[11],
                      volts[12],
                      volts[13],
                      volts['total'],
                      temps['temp1'],
                      temps['temp2'],
                      temps['mosfet'],
                      temps['balance'],
                      capacity,
                      power,
                      soc,
                      current)
            insert(cur, values, "insertplan")
            
            conn.commit()

        #response = binascii.unhexlify(response)
        #print(response)
        
        #response = response.replace('\n','')
        else:
            print("invalid packet")

        time.sleep(5)
        #    res = ""
         #   for x in response:
        #        res += "{:02x}".format(x,2)
        #    print(res)
    #fh.close()
    ser.close()
    disconnect_db(conn)
