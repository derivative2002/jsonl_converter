import argparse
import json
import sys
from pathlib import Path
import logging
from tqdm import tqdm
import concurrent.futures
import os
import ijson

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# 配置日志记录
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

def load_config():
    """加载配置文件。"""
    config_path = Path(__file__).parent.parent / 'config.json'
    if config_path.exists():
        with config_path.open('r', encoding='utf-8') as f:
            config = json.load(f)
            return resolve_paths(config)
    return {}

def resolve_paths(config):
    """解析配置中的相对路径和绝对路径。"""
    project_root = get_project_root()
    for key, value in config.items():
        if isinstance(value, str) and (value.startswith('./') or value.startswith('../')):
            config[key] = (project_root / value).resolve()
        elif isinstance(value, str) and Path(value).is_absolute():
            config[key] = Path(value).resolve()
    return config

def get_project_root():
    """获取项目根目录。"""
    return Path(__file__).parent.parent

def is_valid_json(json_str):
    """检查字符串是否为有效的JSON格式。"""
    try:
        json.loads(json_str)
        return True
    except json.JSONDecodeError:
        return False

def convert_to_serving_format(input_item):
    """将输入项转换为服务格式。"""
    serving_format = {
        "model_control": {
            "system_data": []
        },
        "data": []
    }
    
    if "model_control" in input_item and "system_data" in input_item["model_control"]:
        serving_format["model_control"]["system_data"] = input_item["model_control"]["system_data"]
    
    if "raw_data" in input_item and "data" in input_item["raw_data"]:
        for item in input_item["raw_data"]["data"]:
            serving_format["data"].append({
                "role": item["role"],
                "name": item.get("name", ""),
                "text": item["text"]
            })
    
    return serving_format

def process_json_content(content, file_path):
    try:
        # 解析 JSON 内容
        data = json.loads(content)
        
        # 检查必要的字段
        required_fields = ['id', 'conversations']
        for field in required_fields:
            if field not in data:
                raise ValueError(f"缺少必要字段: {field}")
        
        # 检查 conversations 是否为列表且不为空
        if not isinstance(data['conversations'], list) or len(data['conversations']) == 0:
            raise ValueError("conversations 必须是非空列表")
        
        # 检查每个对话是否包含必要的字段
        for conv in data['conversations']:
            if 'from' not in conv or 'value' not in conv:
                raise ValueError("对话中缺少 'from' 或 'value' 字段")
        
        # 如果所有检查都通过，则转换为服务格式
        return convert_to_serving_format(data)
    
    except json.JSONDecodeError:
        logging.error(f"文件 {file_path} 包含无效的 JSON")
        return None
    except ValueError as ve:
        logging.error(f"文件 {file_path} 中的数据格式错误: {str(ve)}")
        return None
    except Exception as e:
        logging.error(f"处理文件 {file_path} 时发生未知错误: {str(e)}")
        return None

def process_file(file_path, output_dir, max_lines=None):
    try:
        output_path = output_dir / f"{file_path.stem}_processed.jsonl"
        processed_lines = 0
        valid_lines = 0

        with open(file_path, 'rb') as infile, open(output_path, 'w', encoding='utf-8') as outfile:
            # 尝试作为单个大型 JSON 对象处理
            try:
                for item in ijson.items(infile, 'item'):
                    if max_lines and valid_lines >= max_lines:
                        break
                    processed_lines += 1
                    try:
                        processed_content = convert_to_serving_format(item)
                        json.dump(processed_content, outfile, ensure_ascii=False)
                        outfile.write('\n')
                        valid_lines += 1
                    except Exception as e:
                        logging.warning(f"处理项时发生错误: {str(e)}")
            except ijson.JSONError:
                # 如果不是单个大型 JSON，则尝试作为 JSONL 处理
                infile.seek(0)
                for line in tqdm(infile, desc=f"处理 {file_path.name}", unit="行"):
                    if max_lines and valid_lines >= max_lines:
                        break
                    processed_lines += 1
                    try:
                        item = json.loads(line.decode('utf-8').strip())
                        processed_content = convert_to_serving_format(item)
                        json.dump(processed_content, outfile, ensure_ascii=False)
                        outfile.write('\n')
                        valid_lines += 1
                    except json.JSONDecodeError:
                        logging.warning(f"跳过无效的 JSON 行: {line[:50]}...")
                    except Exception as e:
                        logging.warning(f"处理行时发生错误: {str(e)}")

        logging.info(f"文件 {file_path.name} 处理完成. 总行数: {processed_lines}, 有效行数: {valid_lines}")
        return file_path.name, processed_lines, valid_lines
    except Exception as e:
        logging.error(f"处理文件 {file_path} 时发生错误: {str(e)}")
        return file_path.name, 0, 0
    try:
        output_path = output_dir / f"{file_path.stem}_processed.jsonl"
        processed_lines = 0
        valid_lines = 0

        with open(file_path, 'rb') as infile, open(output_path, 'w', encoding='utf-8') as outfile:
            # 检查文件类型
            first_char = infile.read(1)
            infile.seek(0)  # 重置文件指针

            if first_char == b'{':  # JSON 或 JSON.part-* 文件
                for item in ijson.items(infile, 'item'):
                    if max_lines and valid_lines >= max_lines:
                        break
                    processed_lines += 1
                    try:
                        processed_content = convert_to_serving_format(item)
                        json.dump(processed_content, outfile, ensure_ascii=False)
                        outfile.write('\n')
                        valid_lines += 1
                    except Exception as e:
                        logging.error(f"处理项时发生错误: {str(e)}")
            else:  # JSONL 文件
                for line in tqdm(infile, desc=f"处理 {file_path.name}", unit="行"):
                    if max_lines and valid_lines >= max_lines:
                        break
                    processed_lines += 1
                    try:
                        item = json.loads(line.decode('utf-8').strip())
                        processed_content = convert_to_serving_format(item)
                        json.dump(processed_content, outfile, ensure_ascii=False)
                        outfile.write('\n')
                        valid_lines += 1
                    except json.JSONDecodeError:
                        logging.warning(f"跳过无效的 JSON 行: {line[:50]}...")
                    except Exception as e:
                        logging.error(f"处理行时发生错误: {str(e)}")

        logging.info(f"文件 {file_path.name} 处理完成. 总行数/项数: {processed_lines}, 有效行数/项数: {valid_lines}")
        return file_path.name, processed_lines, valid_lines
    except Exception as e:
        logging.error(f"处理文件 {file_path} 时发生错误: {str(e)}")
        return file_path.name, 0, 0

def process_all_files(input_files, output_dir, max_lines):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        future_to_file = {executor.submit(process_file, input_file, output_dir, max_lines): input_file for input_file in input_files}
        
        for future in tqdm(concurrent.futures.as_completed(future_to_file), total=len(input_files), desc="总体进度"):
            results.append(future.result())

    return results

def validate_input_file(file_path):
    if not file_path.exists():
        raise ValueError(f"文件 {file_path} 不存在")
    if not file_path.is_file():
        raise ValueError(f"{file_path} 不是一个文件")
    if not os.access(file_path, os.R_OK):
        raise ValueError(f"没有权限读取文件 {file_path}")


def main():
    config = load_config()
    logging.info(f"加载的配置: {config}")
    
    parser = argparse.ArgumentParser(description='将JSON/JSONL转换为服务格式')
    parser.add_argument('-i', '--input', type=str, help='输入目录或特定文件')
    parser.add_argument('-o', '--output', type=str, help='输出目录')
    parser.add_argument('-m', '--max_lines', type=int, help='每个文件处理的最大有效行数')
    args = parser.parse_args()

    if args.input:
        input_path = Path(args.input)
        if input_path.is_dir():
            input_dir = input_path
        else:
            input_dir = input_path.parent
    else:
        input_dir = Path(config.get('input_dir', 'data/input'))

    if args.output:
        output_dir = Path(args.output)
    else:
        output_dir = Path(config.get('output_dir', 'data/output'))
    
    output_dir.mkdir(parents=True, exist_ok=True)

    max_lines = args.max_lines or config.get('max_output_lines')

    if args.input and Path(args.input).is_file():
        input_files = [Path(args.input)]
    else:
        input_files = list(input_dir.glob('*.json')) + list(input_dir.glob('*.jsonl')) + list(input_dir.glob('*.json.part-*'))
    
    if not input_files:
        logging.error(f"在 {input_dir} 目录中未找到 .json, .jsonl, 或 .json.part-* 文件")
        sys.exit(1)

    for file in input_files:
        try:
            validate_input_file(file)
        except ValueError as e:
            logging.error(str(e))
            sys.exit(1)

    logging.info(f"开始处理 {len(input_files)} 个文件")
    logging.info(f"输入目录: {input_dir}")
    logging.info(f"输出目录: {output_dir}")
    logging.info(f"每个文件最大处理行数: {max_lines if max_lines else '无限制'}")

    results = process_all_files(input_files, output_dir, max_lines)

    total_processed = sum(result[1] for result in results)
    total_valid = sum(result[2] for result in results)
    
    logging.info("所有文件处理完成")
    logging.info(f"总处理行数: {total_processed}")
    logging.info(f"总有效行数: {total_valid}")
    
    for file_name, processed_lines, valid_lines in results:
        logging.info(f"文件 {file_name}: 处理行数 {processed_lines}, 有效行数 {valid_lines}")

if __name__ == "__main__":
    main()
