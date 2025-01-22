#!/bin/sh

genavro(){
	export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc
	cat ./sample.d/sample.jsonl |
		json2avrows |
		cat > ./sample.d/sample.avro
}

#genavro

export ENV_SCHEMA_FILENAME=./sample.d/output.avsc

export ENV_WASM_FUNC_NAME=wh2area
export ENV_WASM_MODULE_DIR=./sample.d/modules.d
export ENV_WASM_MODULE_NAME=compute_area

export ENV_TARGET_COL1=width
export ENV_TARGET_COL2=height
export ENV_NEW_COL_NAME=area

cat ./sample.d/sample.avro |
	./avro-primitive2wasm2newcol |
	rq -aJ |
	jq -c
