import os
import csv

def replace_csv_values(target_folder, ori, tar):
    # 遍历目标文件夹中的所有文件
    for filename in os.listdir(target_folder):
        if filename.endswith('.csv'):
            filepath = os.path.join(target_folder, filename)
            
            # 读取CSV文件内容
            with open(filepath, 'r', newline='') as infile:
                reader = csv.reader(infile)
                rows = list(reader)
            
            # 处理每一行数据
            modified = False
            for row in rows:
                for i in range(len(row)):
                    if row[i] == str(ori):
                        row[i] = str(tar)
                        modified = True
            
            # 如果有修改则写回文件
            if modified:
                with open(filepath, 'w', newline='') as outfile:
                    writer = csv.writer(outfile)
                    writer.writerows(rows)
                print(f'已更新文件: {filename}')
            else:
                print(f'无需修改: {filename}')

if __name__ == "__main__":
    target_folder = "."
    # replace_csv_values(target_folder, 1000, 200)
    # replace_csv_values(target_folder, 600, 100)
    replace_csv_values(target_folder, 100, 60)
    replace_csv_values(target_folder, 200, 140)