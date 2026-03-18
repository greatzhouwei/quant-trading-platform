@echo off
chcp 65001 >nul
echo ========================================
echo 开始同步2025年数据
echo 时间: %date% %time%
echo ========================================

cd /d "D:\app\quant-trading-platform-master\backend"

:: 激活虚拟环境（如果存在）
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
    echo 已激活虚拟环境
) else (
    echo 未找到虚拟环境，使用系统Python
)

:: 执行同步脚本
python scripts\sync_2025_data.py

:: 记录结果
if %errorlevel% equ 0 (
    echo ========================================
    echo 同步成功完成
echo ========================================
) else (
    echo ========================================
    echo 同步失败，错误码: %errorlevel%
    echo ========================================
)

:: 暂停查看结果（可选）
:: pause
