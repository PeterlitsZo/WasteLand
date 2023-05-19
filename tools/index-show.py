#!/usr/bin/env python3

def show_raw(c):
    line_no = 0
    step = 32
    print('    ', end='')
    for i in range(step):
        print(f'{i:2} ', end='')
    print('  ', end='')
    for i in range(step // 2):
        print(f'{i*2:<2} ', end='')
    print()

    print('    ', end='')
    for i in range(step):
        print(f'---', end='')
    print('  ', end='')
    for i in range(step // 2):
        print(f'---', end='')
    print()

    while True:
        print('    ', end='')
        done = False
        for i in range(step):
            inx = line_no * step + i
            if inx == len(c):
                done = True
                break
            print(f'{c[inx]:02x} ', end='')
        print('  ', end='')
        for i in range(step // 2):
            inx = line_no * step + i * 2
            if inx + 1 >= len(c):
                done = True
                break
            cinx1 = chr(c[inx])
            cinx2 = chr(c[inx + 1])
            if not (c[inx] >= 0x20 and c[inx] <= 0x7E):
                cinx1 = '.'
            if not (c[inx + 1] >= 0x20 and c[inx + 1] <= 0x7E):
                cinx2 = '.'
            print(f'{cinx1}{cinx2} ', end='')
        print()
        line_no += 1
        if done:
            break

def show_head(c):
    print('    TYPE: HEAD')
    print(f'    ROOT_NODE_PAGE_ID: {int.from_bytes(c[64 : 64 + 4], "little")}')
    show_raw(c)

def show_leaf(c):
    print('    TYPE: LEAF')

def show_internal(c):
    print('    TYPE: INTERNAL')

with open('index', 'rb') as f:
    block_id = 0
    while True:
        c = f.read(4 * 1024) # 4KB
        if len(c) == 0: # EOF
            break

        print('BLOCK_ID', block_id)
        block_id += 1

        if c[0] == 1:
            show_head(c)
        elif c[0] == 2:
            show_leaf(c)
        elif c[0] == 3:
            show_internal(c)
        else:
            print('    UNKNOWN')
            show_raw(c)

        print()