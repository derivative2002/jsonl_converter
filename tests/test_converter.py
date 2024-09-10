import argparse
import json
import sys
from pathlib import Path
import logging

# 配置日志记录
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def load_config():
    """加载配置文件。"""
    config_path = Path(__file__).parent.parent / 'config.json'
    if config_path.exists():
        with config_path.open('r') as f:
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

    if "data" in input_item["raw_data"]:
        for item in input_item["raw_data"]["data"]:
            serving_format["data"].append({
                "role": item["role"],
                "name": item.get("name", ""),
                "text": item["text"]
            })

    return serving_format


def process_json_content(content, outfile):
    """处理JSON内容并写入输出文件。"""
    if is_valid_json(content):
        try:
            input_item = json.loads(content)
            serving_format = convert_to_serving_format(input_item)
            json.dump(serving_format, outfile, ensure_ascii=False)
            outfile.write('\n')
            return 1
        except Exception as e:
            logging.error(f"Error processing content: {e}")
    else:
        logging.error("Invalid JSON content")
    return 0


def process_file(input_file, output_file, max_lines=None):
    """处理输入文件并生成输出文件。"""
    input_path = Path(input_file)
    output_path = Path(output_file)
    valid_count = 0

    with input_path.open('r', encoding='utf-8') as infile, \
            output_path.open('w', encoding='utf-8') as outfile:
        if input_path.suffix == '.json':
            content = infile.read()
            valid_count = process_json_content(content, outfile)
        else:
            for i, line in enumerate(infile):
                if max_lines and valid_count >= max_lines:
                    break
                line = line.strip()
                valid_count += process_json_content(line, outfile)

    logging.info(f"Processed {valid_count} valid JSON objects")


def process_all_files(input_files, output_dir, max_lines):
    """处理输入目录下的所有json.part-xx文件。"""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for input_file in input_files:
        part_number = input_file.suffix.split('-')[-1]
        output_file = output_dir / f"{input_file.stem}_cleaned.part-{part_number}.jsonl"
        process_file(input_file, output_file, max_lines)


def main():
    """主函数，处理命令行参数并执行文件转换。"""
    config = load_config()
    project_root = get_project_root()

    parser = argparse.ArgumentParser(description='Convert JSON/JSONL to serving format')
    parser.add_argument('-i', '--input', type=str, help='Input directory or specific file')
    parser.add_argument('-o', '--output', type=str, help='Output directory')
    parser.add_argument('-m', '--max_lines', type=int, help='Maximum number of valid lines to process per file')
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
        input_files = list(input_dir.glob('*.json')) + list(input_dir.glob('*.jsonl')) + list(
            input_dir.glob('*.json.part-*'))

    if not input_files:
        logging.error(f"No .json, .jsonl, or .json.part-* files found in {input_dir} directory")
        sys.exit(1)

    process_all_files(input_files, output_dir, max_lines)
    logging.info(f"Conversion complete. Output saved to {output_dir}")


if __name__ == "__main__":
    main()
