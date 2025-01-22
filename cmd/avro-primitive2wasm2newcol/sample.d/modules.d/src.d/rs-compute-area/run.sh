#!/bin/sh

width=42.0
wbits=$(( 0x4045000000000000 ))

height=42.0
hbits=$(( 0x4045000000000000 ))

areabits=$(( 0x409b900000000000 ))

iwasm --function double2bits ./rs_compute_area.wasm ${width}
iwasm --function wh2area ./rs_compute_area.wasm $(( ${wbits} )) $(( ${wbits} ))
iwasm --function bits2double ./rs_compute_area.wasm $(( ${areabits} ))
