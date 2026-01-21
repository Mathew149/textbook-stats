from flask import Flask, render_template, request, send_file, jsonify
import pandas as pd
import os
from datetime import datetime
from werkzeug.utils import secure_filename
import io
import traceback
import gc

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024  # 限制10MB（减少到10MB）
app.config['UPLOAD_FOLDER'] = '/tmp/uploads'

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

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
    try:
        # 使用较小的chunksize和只读必要的列来减少内存
        print("开始读取学生表...")
        df_student_raw = pd.read_excel(
            student_file,
            usecols=lambda x: x in STUDENT_COLUMN_MAP.values() or x in ['学号', '姓名', '学院', '专业', '行政班', 'ISBN'],
            engine='openpyxl'
        )
        
        df_student = map_columns(df_student_raw, STUDENT_COLUMN_MAP)
        del df_student_raw
        gc.collect()
        
        print("开始读取教材表...")
        # 先只读ISBN列和可能的价格列
        df_book_raw = pd.read_excel(book_file, engine='openpyxl', nrows=None)
        
        df_book_with_isbn = map_columns(df_book_raw, {'ISBN': BOOK_COLUMN_MAP['ISBN']})
        
        price_col_name = find_price_column(df_book_with_isbn)
        if not price_col_name:
            raise KeyError("未找到价格列（需包含'折后价'或'折'字）")
        
        # 只保留需要的列
        df_book = df_book_with_isbn[['ISBN', price_col_name]].copy()
        df_book.rename(columns={price_col_name: '折后价'}, inplace=True)
        
        del df_book_raw, df_book_with_isbn
        gc.collect()
        
        # 数据清洗
        df_student.dropna(subset=['学号', 'ISBN'], how='all', inplace=True)
        df_student['学号'] = df_student['学号'].astype(str).str.strip()
        df_student['ISBN'] = df_student['ISBN'].astype(str).str.strip()
        
        df_book.dropna(subset=['ISBN', '折后价'], how='all', inplace=True)
        df_book['ISBN'] = df_book['ISBN'].astype(str).str.strip()
        df_book['折后价'] = pd.to_numeric(df_book['折后价'], errors='coerce')
        df_book = df_book[df_book['折后价'].notna()]
        
        # 只保留有价格的学生记录
        df_student = df_student[df_student['ISBN'].isin(df_book['ISBN'])].copy()
        
        # 价格映射
        price_dict = dict(zip(df_book['ISBN'], df_book['折后价']))
        df_student['单册价格'] = df_student['ISBN'].map(price_dict)
        df_student = df_student[df_student['单册价格'].notna()]
        
        del df_book, price_dict
        gc.collect()
        
        # 学院筛选
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
        
        df_detail = df_filtered[['学号', '姓名', '学院', '专业', '行政班', 'ISBN', '单册价格']].copy()
        df_detail['个人总计'] = df_detail.groupby('学号')['单册价格'].transform('sum')
        
        summary = {
            '统计时间': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            '目标学院关键词': college_name,
            '匹配到的学院': ', '.join(unique_colleges),
            '价格列来源': price_col_name,
            '总学生数': int(len(df_result)),
            '采购总费用': float(df_result['教材采购总费用'].sum()),
            '人均费用': float(df_result['教材采购总费用'].mean())
        }
        
        del df_student, df_filtered
        gc.collect()
        
        print("数据处理完成")
        
        return {
            "summary": summary,
            "result_df": df_result,
            "detail_df": df_detail,
            "success": True
        }
        
    except Exception as e:
        error_msg = traceback.format_exc()
        print(f"错误: {error_msg}")
        return {
            "success": False,
            "error": str(e)
        }

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/health')
def health():
    return jsonify({"status": "ok", "message": "Server is running"})

@app.route('/process', methods=['POST'])
def process():
    student_path = None
    book_path = None
    
    try:
        if 'student_file' not in request.files or 'book_file' not in request.files:
            return jsonify({"success": False, "error": "请上传两个Excel文件"})
        
        student_file = request.files['student_file']
        book_file = request.files['book_file']
        college_name = request.form.get('college_name', '').strip()
        
        if not college_name:
            return jsonify({"success": False, "error": "请输入学院关键词"})
        
        if student_file.filename == '' or book_file.filename == '':
            return jsonify({"success": False, "error": "请选择有效的文件"})
        
        if not (student_file.filename.endswith(('.xlsx', '.xls')) and 
                book_file.filename.endswith(('.xlsx', '.xls'))):
            return jsonify({"success": False, "error": "请上传Excel文件(.xlsx或.xls格式)"})
        
        # 检查文件大小
        student_file.seek(0, os.SEEK_END)
        student_size = student_file.tell()
        student_file.seek(0)
        
        book_file.seek(0, os.SEEK_END)
        book_size = book_file.tell()
        book_file.seek(0)
        
        if student_size > 10 * 1024 * 1024 or book_size > 10 * 1024 * 1024:
            return jsonify({"success": False, "error": "文件过大，单个文件请不要超过10MB"})
        
        # 保存文件
        student_path = os.path.join(app.config['UPLOAD_FOLDER'], 
                                   secure_filename(student_file.filename))
        book_path = os.path.join(app.config['UPLOAD_FOLDER'], 
                                secure_filename(book_file.filename))
        
        student_file.save(student_path)
        book_file.save(book_path)
        
        print(f"文件已保存，开始处理...")
        
        # 处理数据
        result = process_data(student_path, book_path, college_name)
        
        if result['success']:
            # 生成Excel
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                result["result_df"].to_excel(writer, sheet_name='学院汇总', index=False)
                result["detail_df"].to_excel(writer, sheet_name='购买明细', index=False)
                pd.DataFrame([result["summary"]]).to_excel(writer, sheet_name='统计摘要', index=False)
            
            output.seek(0)
            
            # 保存结果
            output_filename = f"统计_{college_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)
            
            with open(output_path, 'wb') as f:
                f.write(output.getvalue())
            
            # 立即删除输入文件
            try:
                os.remove(student_path)
                os.remove(book_path)
                student_path = None
                book_path = None
            except:
                pass
            
            gc.collect()
            
            return jsonify({
                "success": True,
                "summary": result["summary"],
                "download_url": f"/download/{output_filename}"
            })
        else:
            return jsonify(result)
        
    except Exception as e:
        error_detail = traceback.format_exc()
        print(f"异常: {error_detail}")
        return jsonify({"success": False, "error": f"处理出错: {str(e)}"})
    finally:
        # 清理临时文件
        try:
            if student_path and os.path.exists(student_path):
                os.remove(student_path)
            if book_path and os.path.exists(book_path):
                os.remove(book_path)
        except:
            pass
        gc.collect()

@app.route('/download/<filename>')
def download(filename):
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    if not os.path.exists(filepath):
        return jsonify({"error": "文件不存在"}), 404
    
    try:
        return send_file(filepath, as_attachment=True, download_name=filename)
    finally:
        # 下载后删除文件
        try:
            os.remove(filepath)
        except:
            pass

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
