import pandas as pd
import numpy as np
import pathlib, json, argparse, sys


def convert_to_serializable(obj):
    """
    Recursively converts NumPy and Pandas specific types into JSON-serializable Python types.
    """
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, (pd.Timestamp, pd._libs.tslibs.timestamps.Timestamp)):
        return obj.strftime('%d.%m.%Y %H:%M:%S')
    elif pd.isna(obj):
        return None
    elif isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(item) for item in obj]
    else:
        return obj


def convert(pq_path, out_dir: pathlib.Path):
    """
    Reads a Parquet file and converts it to a Line-delimited JSON (JSONL) format.
    """
    out_path = (out_dir / pq_path.name).with_suffix('.json')
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(pq_path)
    with out_path.open('w', encoding='utf-8') as f:
        for rec in df.to_dict(orient='records'):
            serializable_rec = convert_to_serializable(rec)
            f.write(json.dumps(serializable_rec, ensure_ascii=False) + '\n')
    print(f'Converted: {pq_path} -> {out_path}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert Parquet datasets to JSONL format.')
    parser.add_argument('-o', '--output-dir', type=pathlib.Path,
                        default=pathlib.Path('./output/ccs_commits_dataset_json'),
                        help='Directory to store the generated .json files')
    parser.add_argument('input', nargs='?', type=pathlib.Path,
                        default=pathlib.Path('./output/ccs_commits_dataset'),
                        help='Input file or directory to convert (default: ./output/ccs_commits_dataset)')
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    src = args.input
    if src.is_file():
        convert(src, args.output_dir)
    elif src.is_dir():
        for pq in src.rglob('*.parquet'):
            convert(pq, args.output_dir)
    else:
        print(f"Error: Path {src} does not exist.")
        sys.exit(1)