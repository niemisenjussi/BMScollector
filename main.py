import serial
import psycopg2
import os
import numpy as np
import time
import setproctitle
import math
import json
import binascii
import libscrc

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


def parse_cell_voltages(packet, start_offset=3):
    # Updated NEW BMS
    # Reads cell voltages from packet
    # Each Cell voltage is 16 bits
    if len(packet) < 81:
        return
    num_of_cells = 13
    
    result = {}
    total_voltage = 0.0
    cell = 0
    for loc in range(start_offset, (num_of_cells+1) * 2 + start_offset, 2):
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
    names = {'Contact temp1': [3, 4],
             'Contact temp2': [5, 6],
             'Contact temp3': [7, 8],
             'Contact temp4': [9, 10],
             'Contact temp5': [11, 12],
             'Contact temp6': [13, 14],
             'Env temp1': [15, 16],
             'Env temp2': [17, 18],
             'Env temp3': [19, 20],
             'Heatsink temp': [21, 22],
             'Max temp': [23, 24],
             'Lowest temp': [25, 26]}
    #res = ""
    #for x in packet[85:97]:
    #    res += "{:02x}".format(x,2)
    #print(res)
    
    res = {}
    for t_name, position in names.items():
        res[t_name] = round(float((packet[position[0]] << 8) + packet[position[1]])/10 - 40, 2)
    
    print(json.dumps(res, indent=4))
    #mosfet = packet[4] * 256 + packet[5]
    #balance = packet[87+PACKET_OFFSET] * 256 + packet[88+PACKET_OFFSET]
    #temp1 = packet[89+PACKET_OFFSET] * 256 + packet[90+PACKET_OFFSET]
    #temp2 = packet[91+PACKET_OFFSET] * 256 + packet[92+PACKET_OFFSET]
    #temp3 = packet[93+PACKET_OFFSET] * 256 + packet[94+PACKET_OFFSET]
    #temp4 = packet[95+PACKET_OFFSET] * 256 + packet[96+PACKET_OFFSET]
    #print(f"temp1:{temp1} temp2:{temp2} temp3:{temp3} temp4:{temp4} balance:{balance} mosfet:{mosfet}")
    return res #{'temp1':temp1, 'temp2': temp2, 'balance':balance, 'mosfet':mosfet}

def parse_capacity(packet):
    # Fixed
    current_capacity = float(read16float(packet, 35)) / 100
    current_full_capacity = float(read16float(packet, 37)) / 100
    factory_cap = float(read16float(packet, 39)) / 100
    cycle = int(read16float(packet, 41))
    dat = { 'current_capacity': current_capacity,
            'current_full_capacity': current_full_capacity,
            'cycle': cycle,
            'factory_cap': factory_cap
            }
    #print(json.dumps(dat, indent=4))
    return dat
 
def parse_power(packet):
    power = read32int(packet, 105+PACKET_OFFSET)
    print(f"power: {power}W")
    return power

def parse_state_of_charge(packet):
    # Fixed to new
    soc = float(read16float(packet, 31))
    soh = float(read16float(packet, 33))
    print(f"SOC:{soc}% SOH:{soh}")
    return soc, soh

def parse_current(packet):
    # fixed to new BMS
    charge_current = round(float(read16float(packet, 27)) * 0.1, 2)
    discharge_current = round(float(read16float(packet, 29)) * 0.1, 2)

    print(f"charge_current:{charge_current}A, discharge_current:{discharge_current}A")
    return discharge_current - charge_current
    
    
def parse_protection(packet):
    primary = int(read16float(packet, 43))
    sec = int(read16float(packet, 45))
    third = int(read16float(packet, 47))
    
    print("Primary:" + str(parse_alarm(primary)))
    print("Secondary:" + str(parse_alarm(sec)))
    print("Tertiary:" + str(parse_alarm(third)))
    
def parse_alarm(data):
    alarms = {0x0001: 'Single cell over-voltage protection',
              0x0002: 'Single cell under-voltage protection',
              0x0004: 'Total voltage over-voltage protection',
              0x0008: 'Total voltage under-voltage',
              0x0010: 'Charging overcurrent protection',
              0x0020: 'Discharge overcurrent protection',
              0x0040: 'Charging over temperature',
              0x0080: 'Discharge over temperature',
              0x0100: 'Charging low temperature protection',
              0x0200: 'Low temperature protection',
              0x0400: 'Vdelta OP',
              0x0800: 'Res first',
              0x1000: 'SOC up',
              0x2000: 'TMOS OTP',
            }
    
    active = []
    for loc, name in alarms.items():
        if data & loc == loc:
            #print("{:04x}".format(loc, 4))
            active.append(f"({loc})" + name)
    return active
    
    
def balance_status(packet):
    cells = int(read16float(packet, 49))
    cells2 = int(read16float(packet, 51))
    cell_bit = {  0x0001: 'Cell 1',
                  0x0002: 'Cell 2',
                  0x0004: 'Cell 3',
                  0x0008: 'Cell 4',
                  0x0010: 'Cell 5',
                  0x0020: 'Cell 6',
                  0x0040: 'Cell 7',
                  0x0080: 'Cell 8',
                  0x0100: 'Cell 9',
                  0x0200: 'Cell 10',
                  0x0400: 'Cell 11',
                  0x0800: 'Cell 12',
                  0x1000: 'Cell 13',
                  0x2000: 'Cell 14',
                  0x4000: 'Cell 15',
                  0x8000: 'Cell 16'}
      
    for bit, name in cell_bit.items():
        if cells & bit == bit:
            print(f"balance: {name} active")
       # else:
           # print(f"balance: {name} deactivated")

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
    res = float((packet[offset]<<8) + packet[offset+1])
    return res

#ff db db 00 00 00 00 

def gen_packet(packet):
    checksum = libscrc.modbus(bytearray(packet)) #[0x01, 0x03, 0xD0, 0x00, 0x00, 0x26,]))
    # print("Checksum:{:02x}".format((checksum % 0xFFFF),2))
    packet.append(int(checksum & 0xFF))
    packet.append(int(checksum >> 8))
    
    #res = ""
    #for x in packet:
    #    res += "0x{:02x}, ".format(x,2)
    #print("Packet:"+res)
    return packet


def parse_status(packet):
    sys_status1 = packet[3]
    status1 = { 
                0x01: 'Heat',
                0x02: 'Cool',
                0x04: 'AFE1',
                0x08: 'AFE2',
                0x10: 'Balance',
                0x20: 'Sleep',
                0x40: 'Res1',
                0x80: 'Res2',
                }
    for bit, name in status1.items():
        if sys_status1 & bit == bit:
            print(f"sys_status1: {name} 1")
        else:
            print(f"sys_status1: {name} 0")
        
    sys_status2 = packet[4]
    status2 = { 
                0x01: 'BMS_startup',
                0x02: 'Pre MOS',
                0x04: 'CHG_MOS',
                0x08: 'DSG_MOS',
                0x10: 'Pre_relay',
                0x20: 'CHG_relay',
                0x40: 'DSG_relay',
                0x80: 'Main_relay',
                }
    for bit, name in status2.items():
        if sys_status2 & bit == bit:
            print(f"sys_status2: {name} 1")
        else:
            print(f"sys_status2: {name} 0")
            
    func_status1 = packet[7]
    func1 = { 
                0x01: 'AFE2',
                0x02: 'Sleep',
                0x04: 'SocZero',
            }
    for bit, name in func1.items():
        if func_status1 & bit == bit:
            print(f"func1: {name} 1")
        else:
            print(f"func1: {name} 0")
                
    func_status2 = packet[8]
    func2 = { 
                0x01: 'Balance',
                0x02: 'BMS_Source',
                0x04: 'MosRelay',
                0x08: 'Relay',
                0x10: 'SocFixed',
                0x20: 'Heated',
                0x40: 'Cool',
                0x80: 'AFE1',
                }
    for bit, name in func2.items():
        if func_status2 & bit == bit:
            print(f"func2: {name} 1")
        else:
            print(f"func2: {name} 0")


def set_button(ser):
    # Set balance to 1 value
                          # 4354 cmd   data = 1 
    packet = [0x01, 0x06, 0x11, 0x02, 0x00, 0x01]
    packet = gen_packet(packet.copy())
    ser.write(serial.to_bytes(packet))
    response = list(ser.read(size=200))  
    print(response)
    
def balance_read(ser):
    # Read balance settings
                          # 8960 cmd 
    packet = [0x01, 0x03, 0x23, 0x00, 0x00, 0x08]
    packet = gen_packet(packet.copy())
    ser.write(serial.to_bytes(packet))
    response = list(ser.read(size=200))  
    print(response)
    
def other_read(ser):
    # Read charging settings etc
                         # 8968 cmd 
    packet = [0x01, 0x03, 0x23, 0x08, 0x00, 0x08]
    packet = gen_packet(packet.copy())
    ser.write(serial.to_bytes(packet))
    response = list(ser.read(size=200))  
    print("other response")
    print(response)
    
def balance_set(ser):
    # Set balance settings
    
                          # 8960 cmd  
    packet = [0x01, 0x10, 0x23, 0x00, 0x00, 0x08, 0x10, 0x0d, 0xac, #4000mv  => orig 0x0f, 0xa0,   ### 0x0d 0xac => 3500mv
                                                        0x01, 0xf4, #500 => orig 0x01, 0xf4 ### 10mv asetettu nyt
                                                        0x00, 0x1e, # 150  => orig 0x00, 0x96, # 30mv asetus
                                                        0x00, 0x32, # 200 => orig 0x00, 0xc8   # 50mv asetus
                                                        0x00, 0x05, 
                                                        0x00, 0x05,
                                                        0x00, 0x00,
                                                        0x00, 0x00]
    packet = gen_packet(packet.copy())
    ser.write(serial.to_bytes(packet))
    response = list(ser.read(size=200))
    print("Balance set response")
    print(response)
    

if __name__ == "__main__":
    print("Main starts") 
    setproctitle.setproctitle('bmscollector')

    ser = serial.Serial('/dev/ttyUSB0', 19200, timeout=1)


    query = {# 'balance_settings': [0x01, 0x03, 0x23, 0x00, 0x00, 0x08],
             'voltages':      [0x01, 0x03, 0xD0, 0x00, 0x00, 0x26], # 0xFC, 0xD0],
             'temps_current': [0x01, 0x03, 0xD0, 0x26, 0x00, 0x19],
             #'other2':   [0x01, 0x03, 0xD1, 0x00, 0x00, 0x15],
             'status':   [0x01, 0x03, 0xD1, 0x15, 0x00, 0x0C],
             #'other_read': [0x01, 0x03, 0x23, 0x08, 0x00, 0x08],
             #'other4':   [0x01, 0x03, 0xD2, 0x00, 0x00, 0x01],
             #'default':   [0x01, 0x03, 0xD0, 0x00, 0x00, 0x24],
             #'other':         [0x01, 0x03, 0xD0, 0x03, 0x00, 0x07], # , 0xCC, 0xC8],
             #'other':         [0x01, 0x03, 0xD3, 0x00, 0x00, 0x20], # , 0x7C, 0x96],
             #'temps_current': [0x01, 0x03, 0xD0, 0x01, 0x00, 0x19], # , 0xED, 0x00],
             #'random1':       [0x01, 0x03, 0xD0, 0x26, 0x00, 0x19], # , 0x5D, 0x0B],
             #'ramdom2':       [0x01, 0x03, 0xD1, 0x00, 0x00, 0x15], # , 0xBD, 0x39],
             #'ramdom3':       [0x01, 0x03, 0xD1, 0x15, 0x00, 0x0C], # , 0x6D, 0x37],
            }
            
    # other_read
    # resplen:21
    # [1, 3, 16, 11, 184, 11, 184, 9, 196, 9, 196, 1, 244, 1, 144, 1, 244, 1, 144, 3, 86]
    # 01 03 10 0b b8 0b b8 09 c4 09 c4 01 f4 01 90 01 f4 01 90 03 56 
    # p:3 val:3000.0
    # p:5 val:3000.0
    # p:7 val:2500.0
    # p:9 val:2500.0
    # p:11 val:500.0
    # p:13 val:400.0
    # p:15 val:500.0
    # p:17 val:400.0




    conn = connect_db()

    cur = conn.cursor()
    sqlquery = """ prepare insertplan as
                 INSERT INTO
                  "BMS_RAW"
                 VALUES
                  ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, current_timestamp) """
    cur.execute(sqlquery)

    #set_button(ser)
    
    #other_read(ser)
    
    #balance_set(ser)
    
    every_n = 0
    #fh = open('db3.txt', 'w')
    lastunk = 0
    while(True):
        #print("Sending next request")
        #every_n += 1 
        #if every_n == 5:
            #
            #every_n = 0
        voltages = {}
        for queryname, query_data in query.items():
            packet = gen_packet(query_data.copy())
            #print(packet)
            ser.write(serial.to_bytes(packet))
            response = list(ser.read(size=200))            
            checksum = libscrc.modbus(bytearray(response[:-2]))

            if len(response) > 0 and (checksum>>8) == response[-1] and (checksum&0xFF) == response[-2]:
                print(f"\n{queryname}")
                print("resplen:"+str(len(response)))
                print(response)
                res = ""
                
                for x in response:
                    res += "{:02x} ".format(x, 2)
                print(res)
                
                if queryname == 'voltages':
                    voltages = parse_cell_voltages(response)
                elif queryname == 'temps_current':
                    temps = parse_temperature(response)
                    current = parse_current(response)
                    capacity = parse_capacity(response)
                    soc, soh = parse_state_of_charge(response)
                    parse_protection(response)
                    balance_status(response)
                    
                    if voltages != {}:
                        values = (voltages[0],
                                  voltages[1],
                                  voltages[2],
                                  voltages[3],
                                  voltages[4],
                                  voltages[5],
                                  voltages[6],
                                  voltages[7],
                                  voltages[8],
                                  voltages[9],
                                  voltages[10],
                                  voltages[11],
                                  voltages[12],
                                  voltages[13],
                                  voltages['total'],
                                  temps['Contact temp1'],
                                  temps['Contact temp2'],
                                  temps['Heatsink temp'],
                                  temps['Contact temp3'],
                                  capacity['current_capacity'],
                                  round(voltages['total']*current, 2),
                                  soc,
                                  current)

                        insert(cur, values, "insertplan")
                        
                        conn.commit()
                elif queryname == "status":
                    #[1, 3, 24, 36, 12, 0, 0, 2, 135, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 55, 87]
                    #01 03 18 24 0c 00 00 02 87 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 37 57 

                    parse_status(response)

                else:
                    for p in range(3, len(response)-2, 2):
                        val = read16float(response, p)
                        print(f"p:{p} val:{val}")
                    #parse_current(response)
                
        time.sleep(1)
            
            
        # checksum = 0
        # for x in response[:-2]:
            # checksum += x
        # checksum -= 696
        # print("Checksum:"+str(checksum % 0xFFFF))
        # print("Checksum:{:02x}".format((checksum % 0xFFFF),2))

        # unk = response[-4] * 0xFF + response[-3]
        # packetchecksum = response[-2] * 0xFF + response[-1]
        # print("unk:"+str(unk))
        # print("lastunk:"+str(lastunk))
        # print("Lastunkdelta:"+str(unk - lastunk))
        # lastunk = unk
        # print("packetchecksum:"+str(packetchecksum))
        # print("Delta:"+str((checksum % 0xFFFF) - packetchecksum))

       # if len(response) == 140 and response[0:3] == [0xaa, 0x55, 0xaa]:
            

        #else:
        #    print("invalid packet")

        #time.sleep(5)

    ser.close()
    disconnect_db(conn)
