import argparse
import json
import sys
from pathlib import Path

def load_config():
    """加载配置文件。"""
    config_path = Path(__file__).parent.parent / 'config.json'
    if config_path.exists():
        with config_path.open('r') as f:
            config = json.load(f)
            config = resolve_paths(config)
            return config
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
    
    if "data" in input_item["raw_data"]:
        for item in input_item["raw_data"]["data"]:
            serving_format["data"].append({
                "role": item["role"],
                "name": item.get("name", ""),
                "text": item["text"]
            })
    
    return serving_format

def process_file(input_file, output_file, max_lines=None):
    """处理输入文件并生成输出文件。"""
    input_path = Path(input_file)
    output_path = Path(output_file)
    valid_count = 0  # 初始化 valid_count

    with input_path.open('r', encoding='utf-8') as infile, \
         output_path.open('w', encoding='utf-8') as outfile:
        if input_path.suffix == '.json':
            # 处理单个 JSON 对象
            content = infile.read()
            if is_valid_json(content):
                try:
                    input_item = json.loads(content)
                    serving_format = convert_to_serving_format(input_item)
                    json.dump(serving_format, outfile, ensure_ascii=False)
                    outfile.write('\n')
                    valid_count = 1  # 如果 JSON 有效，设置 valid_count 为 1
                except Exception as e:
                    print(f"Error processing file: {e}", file=sys.stderr)
            else:
                print(f"Invalid JSON file: {input_file}", file=sys.stderr)
        else:
            # 处理 JSONL 文件
            for i, line in enumerate(infile):
                if max_lines and valid_count >= max_lines:
                    break
                line = line.strip()
                if is_valid_json(line):
                    try:
                        input_item = json.loads(line)
                        serving_format = convert_to_serving_format(input_item)
                        json.dump(serving_format, outfile, ensure_ascii=False)
                        outfile.write('\n')
                        valid_count += 1
                    except Exception as e:
                        print(f"Error processing line {i+1}: {e}", file=sys.stderr)
                else:
                    print(f"Skipping invalid JSON on line {i+1}", file=sys.stderr)

    print(f"Processed {valid_count} valid JSON objects")

def process_files(input_files, output_dir, max_files=None, max_lines=None):
    """批量处理多个输入文件，每个输入文件生成一个对应的输出文件。"""
    processed_count = 0
    for input_file in input_files:
        if max_files and processed_count >= max_files:
            break
        
        output_file = output_dir / f"{input_file.stem}_cleaned_serving.jsonl"
        print(f"Processing file: {input_file}")
        process_file(input_file, output_file, max_lines)
        processed_count += 1
    
    print(f"Processed {processed_count} files")
    
def main():
    """主函数，处理命令行参数并执行文件转换。"""
    config = load_config()
    project_root = get_project_root()
    
    parser = argparse.ArgumentParser(description='Convert JSON/JSONL to serving format')
    parser.add_argument('-i', '--input', type=str, help='Input directory or specific file')
    parser.add_argument('-o', '--output', type=str, help='Output directory')
    parser.add_argument('-m', '--max_lines', type=int, help='Maximum number of valid lines to process per file')
    parser.add_argument('-f', '--max_files', type=int, help='Maximum number of files to process')
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
    max_files = args.max_files or config.get('max_input_files', 10)  # 使用配置文件中的值，默认为10

    if args.input and Path(args.input).is_file():
        input_files = [Path(args.input)]
    else:
        input_files = list(input_dir.glob('*.json')) + list(input_dir.glob('*.jsonl')) + list(input_dir.glob('*.json.part-*'))
    
    if not input_files:
        print(f"No .json, .jsonl, or .json.part-* files found in {input_dir} directory", file=sys.stderr)
        sys.exit(1)

    process_files(input_files, output_dir, max_files, max_lines)
    print(f"Conversion complete. Output files saved to {output_dir}")

if __name__ == "__main__":
    main()