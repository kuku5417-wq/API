# PDF to Google Drive Automation

`sample_data/` 폴더에 PDF를 push하면 자동으로 Google Drive `sample` 폴더에 업로드됩니다.

## 폴더 구조

```
├── config/
│   ├── googleDrive.py   # Drive 인증 & 서비스
│   └── driveDB.py       # PDF 추출 & 업로드
├── test/
│   └── testDriveDB.py   # 실행 스크립트
├── sample_data/         # ← PDF 파일 여기에 넣기
└── .github/workflows/
    └── pdf_to_drive.yml # GitHub Actions
```

## 사용법

1. `sample_data/` 폴더에 PDF 파일 추가
2. git push → GitHub Actions 자동 실행
3. Google Drive `sample` 폴더에서 결과 확인
