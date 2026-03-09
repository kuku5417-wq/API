import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from config.driveDB import extract_and_upload_pdf

pdf_files = list(Path("sample_data").glob("*.pdf"))

if not pdf_files:
    print("❌ sample_data/ 폴더에 PDF 파일이 없습니다.")
    sys.exit(1)

print(f"📄 PDF {len(pdf_files)}개 발견 → Google Drive 업로드 시작\n")

success, fail = 0, 0
for pdf in pdf_files:
    try:
        print(f"⏳ {pdf.name} 처리 중...")
        result = extract_and_upload_pdf(pdf)
        print(f"✅ {pdf.name} 완료 (folder_id: {result['folder_id']})\n")
        success += 1
    except Exception as e:
        print(f"❌ {pdf.name} 실패: {e}\n")
        fail += 1

print(f"===== 결과: 성공 {success}개 / 실패 {fail}개 =====")
