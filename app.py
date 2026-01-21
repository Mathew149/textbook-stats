from flask import Flask, render_template, request, send_file, jsonify
import pandas as pd
import os
from datetime import datetime
from werkzeug.utils import secure_filename
import io

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 限制50MB
app.config['UPLOAD_FOLDER'] = 'uploads'

# 确保上传目录存在
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# ===== 保留你原来的业务逻辑代码 =====
STUDENT_COLUMN_MAP = {
    '学号': '学号',
    '姓名': '姓名',
    '学院': '学院',
    '专业': '专业',
    '行政班': '行政班',
    'ISBN': 'ISBN'
}

BOOK_COLUMN_MAP = {
    'ISBN': 'ISBN',
}

def map_columns(df, column_map):
    reverse_map = {v: k for k, v in column_map.items()}
    df_renamed = df.rename(columns=reverse_map)
    missing_cols = set(column_map.keys()) - set(df_renamed.columns)
    if missing_cols:
        raise KeyError(f"在数据表中找不到以下列: {list(missing_cols)}")
    return df_renamed

def find_price_column(df):
    for col in df.columns:
        if '折后价' in col or '折后价'.lower() in col.lower():
            return col
    
    discount_cols = [col for col in df.columns if '折' in col or 'discount' in col.lower()]
    if discount_cols:
        return discount_cols[0]
    return None

def process_data(student_file, book_file, college_name):
    """处理数据的核心函数"""
    try:
        # 加载数据
        df_student_raw = pd.read_excel(student_file)
        df_student = map_columns(df_student_raw, STUDENT_COLUMN_MAP)
        
        df_book_raw = pd.read_excel(book_file)
        df_book_with_isbn = map_columns(df_book_raw, {'ISBN': BOOK_COLUMN_MAP['ISBN']})
        
        # 查找价格列
        price_col_name = find_price_column(df_book_with_isbn)
        if not price_col_name:
            raise KeyError("未找到价格列（需包含'折后价'或'折'字）")
        
        df_book = df_book_with_isbn.rename(columns={price_col_name: '折后价'})
        
        # 数据清洗
        df_student.dropna(subset=['学号', 'ISBN'], how='all', inplace=True)
        df_student['学号'] = df_student['学号'].astype(str).str.strip()
        df_student['ISBN'] = df_student['ISBN'].astype(str).str.strip()
        
        df_book.dropna(subset=['ISBN', '折后价'], how='all', inplace=True)
        df_book['ISBN'] = df_book['ISBN'].astype(str).str.strip()
        df_book['折后价'] = pd.to_numeric(df_book['折后价'], errors='coerce')
        df_book = df_book[df_book['折后价'].notna()]
        df_student = df_student[df_student['ISBN'].isin(df_book['ISBN'])]
        
        # 价格映射
        price_dict = dict(zip(df_book['ISBN'], df_book['折后价']))
        df_student['单册价格'] = df_student['ISBN'].map(price_dict)
        df_student = df_student[df_student['单册价格'].notna()]
        
        # 学院筛选（模糊搜索）
        df_filtered = df_student[df_student['学院'].str.contains(college_name, na=False)].copy()
        
        if df_filtered.empty:
            all_colleges = df_student['学院'].unique()[:10]
            raise ValueError(f"未找到包含'{college_name}'的学院。\n可用学院示例: {', '.join(all_colleges)}")
        
        unique_colleges = df_filtered['学院'].unique()
        
        # 汇总计算
        df_result = df_filtered.groupby(
            ['学号', '姓名', '学院', '专业', '行政班'],
            as_index=False
        )['单册价格'].sum()
        df_result.rename(columns={'单册价格': '教材采购总费用'}, inplace=True)
        
        df_detail = df_filtered.copy()
        df_detail['个人总计'] = df_detail.groupby('学号')['单册价格'].transform('sum')
        
        summary = {
            '统计时间': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            '目标学院关键词': college_name,
            '匹配到的学院': ', '.join(unique_colleges),
            '价格列来源': price_col_name,
            '总学生数': len(df_result),
            '采购总费用': float(df_result['教材采购总费用'].sum()),
            '人均费用': float(df_result['教材采购总费用'].mean())
        }
        
        return {
            "summary": summary,
            "result_df": df_result,
            "detail_df": df_detail,
            "success": True
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process():
    try:
        # 检查文件
        if 'student_file' not in request.files or 'book_file' not in request.files:
            return jsonify({"success": False, "error": "请上传两个Excel文件"})
        
        student_file = request.files['student_file']
        book_file = request.files['book_file']
        college_name = request.form.get('college_name', '').strip()
        
        if not college_name:
            return jsonify({"success": False, "error": "请输入学院关键词"})
        
        if student_file.filename == '' or book_file.filename == '':
            return jsonify({"success": False, "error": "请选择有效的文件"})
        
        # 保存文件
        student_path = os.path.join(app.config['UPLOAD_FOLDER'], 
                                   secure_filename(student_file.filename))
        book_path = os.path.join(app.config['UPLOAD_FOLDER'], 
                                secure_filename(book_file.filename))
        
        student_file.save(student_path)
        book_file.save(book_path)
        
        # 处理数据
        result = process_data(student_path, book_path, college_name)
        
        if result['success']:
            # 生成Excel文件
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                result["result_df"].to_excel(writer, sheet_name='学院汇总', index=False)
                result["detail_df"].to_excel(writer, sheet_name='购买明细', index=False)
                pd.DataFrame([result["summary"]]).to_excel(writer, sheet_name='统计摘要', index=False)
            
            output.seek(0)
            
            # 保存到临时文件
            output_filename = f"教材费用统计_{college_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)
            
            with open(output_path, 'wb') as f:
                f.write(output.getvalue())
            
            # 清理上传的文件
            os.remove(student_path)
            os.remove(book_path)
            
            return jsonify({
                "success": True,
                "summary": result["summary"],
                "download_url": f"/download/{output_filename}"
            })
        else:
            return jsonify(result)
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/download/<filename>')
def download(filename):
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    return send_file(filepath, as_attachment=True, download_name=filename)

if __name__ == '__main__':
    # 本地开发模式
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
