import fitz  # PyMuPDF
import io
from googleapiclient.http import MediaIoBaseUpload
from config.googleDrive import get_drive_service, SAMPLE_FOLDER_ID


def extract_and_upload_pdf(pdf_path):
    service = get_drive_service()
    pdf_name = pdf_path.stem

    # PDF별 폴더 생성
    folder_meta = {
        "name": pdf_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [SAMPLE_FOLDER_ID]
    }
    folder = service.files().create(body=folder_meta, fields="id").execute()
    folder_id = folder["id"]

    doc = fitz.open(pdf_path)

    # 텍스트 추출 → text.txt 업로드
    text = "\n".join([page.get_text() for page in doc])
    text_media = MediaIoBaseUpload(io.BytesIO(text.encode("utf-8")), mimetype="text/plain")
    text_file = service.files().create(
        body={"name": "text.txt", "parents": [folder_id]},
        media_body=text_media,
        fields="id"
    ).execute()

    # 이미지 추출 → image_1.png, image_2.png ... 업로드
    image_ids = []
    img_count = 1
    for page in doc:
        for img in page.get_images(full=True):
            xref = img[0]
            base = doc.extract_image(xref)
            img_bytes = base["image"]
            img_ext = base["ext"]
            media = MediaIoBaseUpload(io.BytesIO(img_bytes), mimetype=f"image/{img_ext}")
            img_file = service.files().create(
                body={"name": f"image_{img_count}.{img_ext}", "parents": [folder_id]},
                media_body=media,
                fields="id"
            ).execute()
            image_ids.append(img_file["id"])
            img_count += 1

    print(f"  └ 텍스트: text.txt")
    print(f"  └ 이미지: {len(image_ids)}개")

    return {
        "folder_id": folder_id,
        "text_file_id": text_file["id"],
        "image_file_ids": image_ids
    }
