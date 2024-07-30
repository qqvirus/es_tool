import os
import struct
import zlib
from pathlib import Path
# Constants for translog operations
import re
import json

def extract_json_from_binary_stream(idx, type, binary_stream, writer):
    json_objects = []
    brace_count = 0
    json_str = ''
    in_json = False
    pos = 0
    start_pos = 0
    cur_id = ''
    start_idx_pos = 0
    in_idx = False
    ignore_cnt = 0
    
    for pos in range(0, len(binary_stream)):
        if ignore_cnt > 0:
            ignore_cnt -= 1
            continue
        cur_bin = binary_stream[pos]
        if not in_json and not in_idx and cur_bin == 0x02:
            if binary_stream[pos+1] != 0x07 or binary_stream[pos+2] != 0x14:
                continue
            in_idx = True
            start_idx_pos = pos+3
            ignore_cnt = 2
            continue
        if in_idx:
            if cur_bin == 0x01:
                ignore_cnt = 3
                in_idx = False
                cur_id = binary_stream[start_idx_pos:pos].decode('utf-8', errors='ignore')
                continue
            continue
        if cur_bin == 123:##ord('{'):
            if in_json :
                brace_count += 1
                continue
            else:
               if binary_stream[pos+1] != 34 or binary_stream[pos+2] not in [97,117]:##ord('"'):
                   continue
               in_json = True
               start_pos = pos
               if cur_id == '':
                    print(f"=??{start_pos}==>{json_str}\n")
               continue
        if  cur_bin == 125:##b'}':
            if not in_json:continue
            if brace_count > 0:
                brace_count -= 1
                continue
            in_json = False
            json_str = binary_stream[start_pos:pos+1].decode('utf-8', errors='ignore')
            try:
                if cur_id == '':
                    print(f"=#{start_pos}==>{json_str}\n")
                    continue
                json_obj = json.loads(json_str)
                json_obj = {"_index":idx, "_type":type,"_id":cur_id, "_source":json_obj}
                cur_id = ''
            except json.JSONDecodeError:
                 # 跳过无效的JSON字符串
                print(f"=@{start_pos}==>{json_str}\n")
                continue
            json.dump(json_obj, writer, separators=(',', ':'))
            writer.write('\n')
    return json_objects


    for char in data_str:
        if char == '{':
            if brace_count == 0:
                in_json = True
            brace_count += 1
        if in_json:
            json_str += char
        if char == '}':
            brace_count -= 1
            if brace_count == 0:
                in_json = False
                try:
                    json_obj = json.loads(json_str)
                    json_objects.append(json_obj)
                except json.JSONDecodeError:
                    pass  # 跳过无效的JSON字符串
                json_str = ''
    
    return json_objects



def parse_translog(idx, translog_file, save_dir):
    tf = Path(translog_file)
    save_path = os.path.join(save_dir, idx, tf.stem+".txt")
    tf = Path(save_path)
    if not os.path.exists(tf.parent):os.makedirs(tf.parent)
    with open(translog_file, 'rb') as f, open(save_path, 'w') as writer:
        extract_json_from_binary_stream(idx, "d",f.read(), writer)
        

def fetch_translog_files(translog_dir):
    translog_files = []
    for _, dirs, _ in os.walk(translog_dir):
        for dir in dirs:
            dirpath = os.path.join(translog_dir, dir)
            translog_files.append((dir, [os.path.join(dirpath, f) for f in os.listdir(dirpath) if f.startswith('translog')]))
    return translog_files

def main():
    translog_dir = 'faild'  # 设置你的translog目录路径,es-index-x/translog-xxxx.tlog
    for idx, translog_files in fetch_translog_files(translog_dir):
        for translog_file in translog_files:
            if not translog_file.endswith(".tlog"):continue
            print(f"Parsing translog file: {translog_file}")
            # 保存解析后的json数据至recover中es-index-x目录中同名txt
            # 后可通过elasticdump进行恢复
            # node安装elasticdump
            # 例如node_modules\.bin\elasticdump --input=../recover/es-index-x/translog-xxxx.txt --output=http://xx:9200/es-index-x --limit=1000 --type=data
            parse_translog(idx, translog_file, "recover")

if __name__ == "__main__":
    main()
