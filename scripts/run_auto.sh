#!/bin/bash

files=(
    "70%_high_Fb"
    "70%_high_NoFb"
    "70%_low_Fb"
    "70%_low_NoFb"
    "70%_q3_high_Fb"
    "70%_q3_high_NoFb"
    "70%_q3_low_Fb"
    "70%_q3_low_NoFb"
    "Com"
    "Com_Noc"
    "High_Cidat"
    "High_OCI"
    "High_StaCI"
    "Low_Cidat"
    "Low_OCI"
    "Low_StaCI"
    "NOC_Fla"
    "q2_NOC_la"
    "q2_UNC_la"
    "q3_Com_NOC"
    "q3_Com_UNC"
    "q3_high_OCI"
    "q3_high_StaCI"
    "q3_low_OCI"
    "q3_low_StaCI"
    "q3_NOC_Fla"
    "q3_NOC_la"
    "q3_UNC_Fla"
    "q3_UNC_la"
    "source_df"
    "source_filnk"
    "source_q3_df"
    "source_q3_flink"
    "UNC_Fla"
    "z70%_NOC_Cf_La"
    "z70%_NOC_Sf_La"
    "z70%_q3_NOC_Cf_La"
    "z70%_q3_NOC_Sf_La"
    "z70%_q3_UNC_Cf_Ab_Rt"
    "z70%_q3_UNC_Cf_Fb_Rt"
    "z70%_q3_UNC_Cf_La"
    "z70%_q3_UNC_Sf_Ab_Rt"
    "z70%_q3_UNC_Sf_Fb_Rt"
    "z70%_q3_UNC_Sf_La"
    "z70%_UNC_Cf_Ab_Rt"
    "z70%_UNC_Cf_Fb_Rt"
    "z70%_UNC_Cf_La"
    "z70%_UNC_Sf_Ab_Rt"
    "z70%_UNC_Sf_Fb_Rt"
    "z70%_UNC_Sf_La"
)

mkdir -p ../saving_res/logs/

for file in "${files[@]}"; do
    log_file="../saving_res/logs/$file.logs"
    
    if [ -f "$log_file" ]; then
        echo "[跳过] 日志已存在: $file"
        continue
    fi
    
    echo "[开始处理] $file"
    bash run_batch_exp.sh "$file" > "$log_file" 2>&1
    
    sleep 600
done
