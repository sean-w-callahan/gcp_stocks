#! /bin/bash

source testvenv/bin/activate

key=$(head -1 key.txt)
export API_KEY=$key
export DAY=$(date -d "-4 days" '+%Y-%m-%d')

pytest test_function.py >> test_results.txt

