"""
사고사례 더미데이터 생성 + PDF 생성 + Google Drive 업로드
- accident.csv : 50건 사고사례 (id, date, summary, cause, result, countermeasure,
                                  accident_type, work_keywords, source, pdf_filename)
- output/accident_pdf/ : 사고사례별 1페이지 PDF
- Drive sample/mssql/accident_pdf/ : PDF 업로드
"""

import io
import os
from pathlib import Path

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (HRFlowable, Paragraph, SimpleDocTemplate,
                                Spacer, Table, TableStyle)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

FOLDER_ID     = "1pfW8vCXMdOz-2zcmfif-FuHRSm6Zsdl5"  # sample/mssql
KEY_PATH      = Path(__file__).parent / ".streamlit" / "tbmsample-7eaea86951af.json"
OUT_DIR       = Path(__file__).parent / "output"
PDF_DIR       = OUT_DIR / "accident_pdf"
PDF_DIR.mkdir(parents=True, exist_ok=True)

# ── 한글 폰트 등록 ─────────────────────────────────────────────────────────────
_FONT_PATHS = [
    "C:/Windows/Fonts/malgun.ttf",
    "C:/Windows/Fonts/gulim.ttc",
]
_FONT_NAME = "Korean"
for _fp in _FONT_PATHS:
    if os.path.exists(_fp):
        try:
            pdfmetrics.registerFont(TTFont(_FONT_NAME, _fp))
            break
        except Exception:
            continue

# ── 사고사례 데이터 ────────────────────────────────────────────────────────────
ACCIDENTS = [
    # LNG작업 (10건)
    {
        "date": "2024-03", "work_keywords": "LNG작업",
        "accident_type": "폭발",
        "summary": "LNG 운반선 화물창 내부 점검 작업 중 잔류 가스가 폭발하여 작업자 2명이 중상을 입었다. 작업 전 가스 퍼지 절차가 미완료된 상태에서 작업이 진행되었다.",
        "cause": "가스 퍼지 절차 미완료, 작업 전 가스 농도 미측정, 감독자 확인 없이 작업 개시",
        "result": "작업자 2명 중상 (화상, 폭발 충격), 화물창 내부 설비 손상",
        "countermeasure": "작업 전 가스 퍼지 완료 확인 의무화, 가스 농도 측정 후 작업 허가 발행, 감독자 입회 하 작업 개시",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2023-11", "work_keywords": "LNG작업",
        "accident_type": "화재",
        "summary": "LNG 연료 공급 라인 플랜지 교체 작업 중 잔류 가스가 누출되어 화재가 발생했다. 작업 구역 내 화기 통제가 미흡하였다.",
        "cause": "잔류 가스 배출 미완료, 화기 작업 동시 진행, 가스 감지기 미작동",
        "result": "작업자 1명 화상(2도), 배관 일부 손상, 인근 작업 일시 중단",
        "countermeasure": "LNG 라인 작업 시 완전 퍼지 후 질소 치환 확인, 인접 화기 작업 동시 금지, 가스 감지기 정기 점검",
        "source": "산업재해분석 보고서 (고용노동부)",
    },
    {
        "date": "2024-01", "work_keywords": "LNG작업",
        "accident_type": "질식",
        "summary": "LNG 탱크 내부 점검 중 산소 결핍 환경에 노출되어 작업자 1명이 의식을 잃었다. 공기호흡기 미착용 상태로 진입하였다.",
        "cause": "밀폐공간 내 산소농도 미측정, 공기호흡기 미착용, 감시인 미배치",
        "result": "작업자 1명 산소결핍으로 의식 상실, 응급 후송 후 회복",
        "countermeasure": "LNG 탱크 진입 전 산소농도 측정 의무화, 공기호흡기 착용 필수, 외부 감시인 1명 이상 배치",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2023-08", "work_keywords": "LNG작업",
        "accident_type": "동상",
        "summary": "LNG 극저온 배관 보냉재 교체 작업 중 액체 LNG가 튀어 작업자 손에 극저온 화상을 입혔다.",
        "cause": "극저온 방호장갑 미착용, 작업 전 배관 내 잔류 LNG 제거 미완료",
        "result": "작업자 1명 손 부위 극저온 화상(2도), 1주일 입원",
        "countermeasure": "극저온 절연 방호장갑 착용 의무화, 작업 전 배관 내 LNG 완전 배출 확인, 개인보호구 착용 점검",
        "source": "선박해양플랜트연구소 안전사례집",
    },
    {
        "date": "2024-05", "work_keywords": "LNG작업",
        "accident_type": "폭발",
        "summary": "LNG 재기화 설비 시운전 중 압력 초과로 안전밸브가 작동하였으나 배출 가스가 점화원에 접촉하여 소규모 폭발이 발생하였다.",
        "cause": "점화원 통제 미흡, 안전밸브 배출 방향 설계 오류, 작업구역 내 화기 존재",
        "result": "작업자 없이 설비 손상만 발생, 재기화 설비 일부 교체",
        "countermeasure": "안전밸브 배출 방향 안전구역으로 변경, 시운전 구역 내 화기 작업 전면 금지, 점화원 통제 체크리스트 강화",
        "source": "한국가스안전공사 사고사례",
    },
    {
        "date": "2023-06", "work_keywords": "LNG작업",
        "accident_type": "누출",
        "summary": "LNG 펌프 씰 교체 작업 후 시운전 중 씰 불량으로 가스가 누출되었다. 감지기가 작동하여 인명 피해는 없었다.",
        "cause": "씰 교체 후 누설 시험 생략, 조립 토크 미준수",
        "result": "인명 피해 없음, 작업 구역 비상 대피 실시, 씰 재교체",
        "countermeasure": "씰 교체 후 누설 시험 의무화, 조립 토크 체크리스트 작성 및 확인, 시운전 전 가스 감지기 작동 확인",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2024-02", "work_keywords": "LNG작업",
        "accident_type": "화재",
        "summary": "LNG 연료 공급 시스템 배관 용접 작업 중 인근 단열재에 불꽃이 튀어 화재가 발생하였다.",
        "cause": "용접 전 인화성 물질 제거 미실시, 화재 감시인 미배치",
        "result": "작업자 1명 경상(화상), 단열재 일부 소실",
        "countermeasure": "용접 전 인근 가연성 물질 제거 또는 차폐, 화재 감시인 배치 의무화, 소화기 대기",
        "source": "조선해양산업 안전사고 분석집",
    },
    {
        "date": "2023-09", "work_keywords": "LNG작업",
        "accident_type": "추락",
        "summary": "LNG 탱크 돔부 작업 중 작업자가 발판에서 미끄러져 약 3m 추락하였다.",
        "cause": "돔부 발판 표면 서리(결빙)로 미끄러움, 안전대 미체결",
        "result": "작업자 1명 다리 골절, 입원 치료 3주",
        "countermeasure": "극저온 구역 발판 미끄럼 방지 처리, 안전대 체결 상태 작업 전 확인, 결빙 여부 점검 후 작업 허가",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2024-07", "work_keywords": "LNG작업",
        "accident_type": "끼임",
        "summary": "LNG 카고 펌프 분해 작업 중 펌프 임펠러와 하우징 사이에 손이 끼어 손가락 2개를 절단하였다.",
        "cause": "LOTO 절차 미적용, 작업 중 펌프 잔류 압력에 의한 갑작스러운 회전",
        "result": "작업자 1명 손가락 2개 절단, 응급 수술 후 부분 회복",
        "countermeasure": "카고 펌프 분해 전 LOTO 절차 철저 적용, 잔류 압력 완전 해제 확인, 보호 장갑 착용 의무화",
        "source": "고용노동부 산업재해 현황",
    },
    {
        "date": "2023-12", "work_keywords": "LNG작업",
        "accident_type": "화재",
        "summary": "LNG 가스 공급 계통 점검 작업 후 연결부 체결 불량으로 가스가 미세 누출되다 점화원에 접촉하여 소규모 화재가 발생하였다.",
        "cause": "연결부 최종 토크 확인 미실시, 점화원 통제 구역 설정 미흡",
        "result": "인명 피해 없음, 배관 연결부 손상, 작업 중단 4시간",
        "countermeasure": "배관 연결 작업 후 누설 시험 의무화, 점화원 통제 구역 확대, 작업 종료 후 최종 점검 절차 강화",
        "source": "한국가스안전공사 사고사례",
    },

    # 고압작업 (8건)
    {
        "date": "2023-05", "work_keywords": "고압작업",
        "accident_type": "폭발",
        "summary": "고압 공기 계통 배관 플랜지 체결 작업 중 잔류 압력에 의해 플랜지가 분리되어 파편이 비산하였다.",
        "cause": "작업 전 계통 압력 미배출, 차단밸브 확인 절차 생략",
        "result": "작업자 1명 얼굴 부위 파편 부상, 인근 설비 손상",
        "countermeasure": "플랜지 해체 전 계통 압력 완전 배출 확인, 차단밸브 잠금 후 LOTO 태그 부착, 안면 보호구 착용 의무화",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2024-04", "work_keywords": "고압작업",
        "accident_type": "폭발",
        "summary": "고압 유압 호스 교체 작업 중 호스 연결부가 탈거되어 고압 오일이 분사되었다.",
        "cause": "호스 연결 시 계통 압력 미해제, 연결 피팅 규격 불일치",
        "result": "작업자 2명 고압 오일 분사 부상 (피부 관통상), 즉시 응급 치료",
        "countermeasure": "유압 호스 교체 전 계통 압력 완전 해제, 피팅 규격 사전 확인, 안면 보호구 및 내압 장갑 착용",
        "source": "조선해양산업 안전사고 분석집",
    },
    {
        "date": "2023-10", "work_keywords": "고압작업",
        "accident_type": "부상",
        "summary": "공기압 계통 압력 시험 중 배관 용접부 결함으로 파열이 발생하였다.",
        "cause": "용접부 비파괴검사 미실시, 시험 압력 초과",
        "result": "작업자 없이 설비 파손만 발생, 인근 작업자 대피로 인명 피해 없음",
        "countermeasure": "압력 시험 전 용접부 비파괴검사 의무화, 시험 압력 단계적 승압, 시험 구역 안전 거리 확보",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2024-06", "work_keywords": "고압작업",
        "accident_type": "부상",
        "summary": "고압 질소를 이용한 배관 기밀 시험 중 질소 공급 호스가 이탈하여 채찍질 현상이 발생하였다.",
        "cause": "호스 고정 클램프 미설치, 연결 피팅 규격 불일치",
        "result": "작업자 1명 호스 채찍질에 의한 타박상, 경상",
        "countermeasure": "고압 호스 사용 시 고정 클램프 설치 의무화, 연결 피팅 규격 일치 여부 확인, 작업 구역 내 비인가자 접근 금지",
        "source": "고용노동부 산업재해 현황",
    },
    {
        "date": "2023-07", "work_keywords": "고압작업",
        "accident_type": "폭발",
        "summary": "에어컴프레서 압력 탱크 안전밸브 점검 작업 중 압력 초과로 탱크가 파열되었다.",
        "cause": "안전밸브 고착으로 정상 작동 불가, 압력 게이지 오작동으로 과압 감지 실패",
        "result": "작업자 1명 중상, 압력 탱크 전파, 주변 설비 손상",
        "countermeasure": "안전밸브 정기 점검 및 작동 시험 의무화, 압력 게이지 교정 주기 준수, 과압 방지 장치 이중화",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2024-03", "work_keywords": "고압작업",
        "accident_type": "부상",
        "summary": "고압 증기 배관 밸브 패킹 교체 작업 중 잔류 증기가 분출되어 작업자가 화상을 입었다.",
        "cause": "계통 증기 완전 배출 미확인, 드레인 밸브 미개방 상태에서 작업 진행",
        "result": "작업자 1명 상반신 증기 화상(2도), 입원 치료 2주",
        "countermeasure": "증기 계통 작업 전 드레인 밸브 개방 후 압력 완전 해제 확인, 내열 보호복 착용, 감독자 입회 의무화",
        "source": "조선해양산업 안전사고 분석집",
    },
    {
        "date": "2023-12", "work_keywords": "고압작업",
        "accident_type": "부상",
        "summary": "유압 잭을 이용한 대형 축 정렬 작업 중 유압이 급격히 해제되어 작업자 발등을 충격하였다.",
        "cause": "유압 릴리프 밸브 급격 조작, 발 위치 안전 확인 미실시",
        "result": "작업자 1명 발 골절, 입원 치료 4주",
        "countermeasure": "유압 해제 시 서서히 조작, 작업 중 안전화 착용 의무화, 신체 위치 확인 후 유압 조작",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2024-08", "work_keywords": "고압작업",
        "accident_type": "폭발",
        "summary": "냉동 냉각 계통 고압 부분 용접 후 기밀 시험 중 용접부 핀홀로 냉매가 누출되어 소규모 폭발이 발생하였다.",
        "cause": "용접 품질 검사 미실시, 기밀 시험 전 용접부 육안 확인 생략",
        "result": "인명 피해 없음, 냉동 계통 일부 손상, 냉매 누출",
        "countermeasure": "용접 후 비파괴검사 필수 실시, 기밀 시험 전 용접부 외관 검사, 냉매 누출 감지기 설치",
        "source": "고용노동부 산업재해 현황",
    },

    # 밀폐작업 (8건)
    {
        "date": "2023-04", "work_keywords": "밀폐작업",
        "accident_type": "질식",
        "summary": "이중저 탱크 내부 도장 작업 중 유기용제 증기가 축적되어 작업자 2명이 의식을 잃었다.",
        "cause": "밀폐공간 환기 미실시, 유기용제 증기 농도 미측정, 공기호흡기 미착용",
        "result": "작업자 2명 중독으로 의식 상실, 응급 후송 후 1명 중상",
        "countermeasure": "도장 작업 전 강제 환기 30분 이상 실시, 유기용제 농도 측정 후 작업 허가, 공기호흡기 착용 의무화",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2024-02", "work_keywords": "밀폐작업",
        "accident_type": "질식",
        "summary": "밸러스트 탱크 내부 청소 작업 중 산소 결핍 환경에서 작업자 1명이 쓰러졌다. 구조를 위해 진입한 동료도 함께 쓰러졌다.",
        "cause": "진입 전 산소농도 미측정, 2차 구조자도 보호구 없이 진입",
        "result": "작업자 2명 산소결핍 의식 상실, 1명 중상 1명 경상",
        "countermeasure": "밀폐공간 진입 전 산소농도 필수 측정, 구조 시에도 공기호흡기 착용 의무화, 외부 감시인 상시 배치",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2023-09", "work_keywords": "밀폐작업",
        "accident_type": "화재",
        "summary": "연료유 탱크 내부 배관 용접 작업 중 잔류 유증기가 점화되어 화재가 발생하였다.",
        "cause": "탱크 내 유증기 제거 미완료, 폭발하한 농도 이상에서 용접 작업 진행",
        "result": "작업자 1명 화상(3도), 탱크 내부 손상",
        "countermeasure": "탱크 내부 용접 전 유증기 농도 폭발하한의 10% 이하 확인, 가스 감지기 작동 중 작업 진행, 대피 경로 사전 확보",
        "source": "조선해양산업 안전사고 분석집",
    },
    {
        "date": "2024-05", "work_keywords": "밀폐작업",
        "accident_type": "추락",
        "summary": "체인 로커 내부 청소 작업 중 좁은 공간에서 발을 헛디뎌 2m 아래로 추락하였다.",
        "cause": "밀폐공간 내 안전 발판 미설치, 조명 불충분으로 시야 불량",
        "result": "작업자 1명 갈비뼈 골절 및 타박상, 입원 2주",
        "countermeasure": "밀폐공간 작업 전 안전 발판 설치, 조명 충분히 확보, 진입 전 내부 구조 확인",
        "source": "고용노동부 산업재해 현황",
    },
    {
        "date": "2023-11", "work_keywords": "밀폐작업",
        "accident_type": "질식",
        "summary": "이너텍 가스 치환된 화물창 내 점검 작업 중 질소 가스 잔류로 산소 결핍 발생하여 작업자가 쓰러졌다.",
        "cause": "가스 치환 후 산소농도 재측정 미실시, 진입 전 공기호흡기 미착용",
        "result": "작업자 1명 의식 상실 후 응급 후송, 완전 회복",
        "countermeasure": "화물창 진입 전 산소농도 반드시 재측정, 불활성 가스 치환 후 작업 시 공기호흡기 필수 착용",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2024-01", "work_keywords": "밀폐작업",
        "accident_type": "중독",
        "summary": "펌프룸 내부 작업 중 황화수소 가스가 발생하여 작업자 1명이 의식을 잃었다.",
        "cause": "황화수소 발생 가능성 사전 평가 미실시, 가스 감지기 미휴대",
        "result": "작업자 1명 황화수소 중독, 응급 후송 후 회복",
        "countermeasure": "펌프룸 진입 전 황화수소 농도 측정, 개인 가스 감지기 휴대 의무화, 감시인 배치",
        "source": "조선해양산업 안전사고 분석집",
    },
    {
        "date": "2023-06", "work_keywords": "밀폐작업",
        "accident_type": "부상",
        "summary": "밀폐된 보이드 스페이스 내에서 작업 중 환기 미흡으로 고온 환경이 조성되어 작업자가 열사병에 걸렸다.",
        "cause": "여름철 밀폐공간 내 환기 부족, 작업 시간 과다, 수분 보충 미흡",
        "result": "작업자 1명 열사병으로 의식 저하, 응급 처치 후 회복",
        "countermeasure": "밀폐공간 기온 측정 후 작업 허가, 환기 팬 추가 설치, 연속 작업 30분 이내 제한 및 휴식 의무화",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2024-09", "work_keywords": "밀폐작업",
        "accident_type": "감전",
        "summary": "탱크 내부 조명 설치 작업 중 습기로 인해 전기 단락이 발생하여 작업자가 감전되었다.",
        "cause": "밀폐공간 내 습기 환경에서 일반 전기 기구 사용, 절연 장갑 미착용",
        "result": "작업자 1명 감전 부상, 심장 박동 일시 이상 후 회복",
        "countermeasure": "밀폐공간 내 방수 전기 기구 사용, 절연 장갑 착용 의무화, 접지 상태 확인 후 전원 인가",
        "source": "고용노동부 산업재해 현황",
    },

    # 시운전작업 (8건)
    {
        "date": "2023-08", "work_keywords": "시운전작업",
        "accident_type": "부상",
        "summary": "주기관 시운전 중 진동에 의해 배관 지지대가 탈락하여 작업자의 발등을 가격하였다.",
        "cause": "시운전 전 배관 지지대 체결 상태 확인 미실시, 진동 허용 한계 초과",
        "result": "작업자 1명 발등 골절, 입원 치료 3주",
        "countermeasure": "시운전 전 배관 지지대 전수 점검, 시운전 중 진동 모니터링, 위험 구역 출입 통제",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2024-03", "work_keywords": "시운전작업",
        "accident_type": "화재",
        "summary": "발전기 시운전 중 연료 공급 라인에서 누출이 발생하여 배기관 접촉으로 화재가 발생하였다.",
        "cause": "연료 라인 연결부 체결 불량, 시운전 전 누설 점검 미실시",
        "result": "발전기 일부 손상, 작업자 대피로 인명 피해 없음",
        "countermeasure": "시운전 전 연료 라인 누설 시험 의무화, 연료 라인 연결부 최종 체결 확인, 소화기 대기",
        "source": "조선해양산업 안전사고 분석집",
    },
    {
        "date": "2023-11", "work_keywords": "시운전작업",
        "accident_type": "끼임",
        "summary": "보조기기 시운전 준비 중 회전체 점검 시 전원이 인가되어 작업자 손이 끼였다.",
        "cause": "LOTO 미적용 상태에서 회전체 점검, 제3자에 의한 오조작으로 전원 인가",
        "result": "작업자 1명 손가락 2개 절단",
        "countermeasure": "회전체 점검 시 LOTO 필수 적용, 작업 중 전원반에 잠금 및 태그 부착, 작업자 간 의사소통 강화",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2024-06", "work_keywords": "시운전작업",
        "accident_type": "추락",
        "summary": "항해 시험 중 갑판 위에서 계측 작업을 하던 작업자가 갑작스러운 선체 동요로 추락하였다.",
        "cause": "해상 시험 중 안전대 미착용, 방호 난간 미설치 구역에서 작업",
        "result": "작업자 1명 갑판 추락으로 다리 골절, 즉각 응급 처치",
        "countermeasure": "항해 시험 시 외부 작업 금지 원칙, 불가피한 경우 안전대 착용 의무화, 난간 설치 확인",
        "source": "선박해양플랜트연구소 안전사례집",
    },
    {
        "date": "2023-07", "work_keywords": "시운전작업",
        "accident_type": "화상",
        "summary": "보일러 시운전 중 고온 증기 배관의 보온재 시공 불량 부위에 작업자가 접촉하여 화상을 입었다.",
        "cause": "보온재 시공 불량으로 고온 배관 노출, 고온 표면 경고 표시 미부착",
        "result": "작업자 1명 팔 부위 화상(2도), 입원 치료 1주",
        "countermeasure": "시운전 전 보온재 시공 상태 전수 점검, 고온 배관 경고 표시 부착, 내열 보호복 착용",
        "source": "고용노동부 산업재해 현황",
    },
    {
        "date": "2024-01", "work_keywords": "시운전작업",
        "accident_type": "부상",
        "summary": "조타장치 기능 시험 중 타기가 예상치 못하게 작동하여 작업자의 팔을 가격하였다.",
        "cause": "조타장치 시험 중 작업자 위치 안전 확인 미실시, 이동 범위 내 작업자 존재",
        "result": "작업자 1명 팔 골절, 입원 치료 3주",
        "countermeasure": "조타장치 시험 전 이동 범위 내 작업자 퇴거 확인, 위험 구역 표시 및 출입 통제, 시험 전 방송 안내",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2023-05", "work_keywords": "시운전작업",
        "accident_type": "감전",
        "summary": "전기 절연저항 측정 작업 중 활선 상태의 케이블에 접촉하여 감전되었다.",
        "cause": "측정 전 전원 차단 미확인, 활선 작업용 보호구 미착용",
        "result": "작업자 1명 감전 경상, 심전도 검사 후 이상 없음",
        "countermeasure": "절연저항 측정 전 반드시 전원 차단 확인, 활선 경고 표시 부착, 절연 장갑 및 안전화 착용",
        "source": "조선해양산업 안전사고 분석집",
    },
    {
        "date": "2024-09", "work_keywords": "시운전작업",
        "accident_type": "부상",
        "summary": "냉각수 계통 기밀 시험 중 과압으로 플랜지 가스켓이 파손되어 냉각수가 분사되었다.",
        "cause": "시험 압력 설정 오류, 압력 게이지 오작동",
        "result": "작업자 1명 냉각수 분사로 경상, 냉각수 약 200L 유출",
        "countermeasure": "기밀 시험 압력 설정 이중 확인, 압력 게이지 정기 교정, 시험 시 작업자 안전 거리 확보",
        "source": "고용노동부 산업재해 현황",
    },

    # 발전기가동 (6건)
    {
        "date": "2024-02", "work_keywords": "발전기가동",
        "accident_type": "화재",
        "summary": "발전기 초기 기동 시 연료 공급 펌프 씰 손상으로 연료유가 누출되어 배기관 접촉으로 화재가 발생하였다.",
        "cause": "연료 펌프 씰 노후화, 가동 전 씰 상태 점검 미실시",
        "result": "발전기실 소규모 화재, 작업자 대피 후 소화기로 진화, 인명 피해 없음",
        "countermeasure": "발전기 가동 전 연료 계통 씰 상태 점검, 연료 누설 감지기 설치, 소화기 상시 대기",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2023-10", "work_keywords": "발전기가동",
        "accident_type": "폭발",
        "summary": "비상발전기 부하 시험 중 배터리 충전 과정에서 수소 가스가 발생하여 소규모 폭발이 발생하였다.",
        "cause": "배터리실 환기 불량, 충전 중 수소 가스 축적",
        "result": "배터리 손상, 배터리실 일부 손상, 인명 피해 없음",
        "countermeasure": "배터리 충전 중 환기 팬 가동 확인, 배터리실 수소 가스 감지기 설치, 충전 완료 후 환기 실시",
        "source": "조선해양산업 안전사고 분석집",
    },
    {
        "date": "2024-07", "work_keywords": "발전기가동",
        "accident_type": "감전",
        "summary": "발전기 가동 중 냉각수 누출로 인해 발전기 외함이 충전되어 작업자가 감전되었다.",
        "cause": "냉각수 누출로 절연 저하, 접지 불량",
        "result": "작업자 1명 감전 중상, 병원 이송 후 회복",
        "countermeasure": "발전기 외함 접지 상태 정기 점검, 냉각수 누설 감지기 설치, 발전기 가동 중 점검 시 절연 장갑 착용",
        "source": "고용노동부 산업재해 현황",
    },
    {
        "date": "2023-06", "work_keywords": "발전기가동",
        "accident_type": "끼임",
        "summary": "발전기 가동 중 냉각 팬 벨트 교체 작업을 시도하다 회전체에 손이 끼였다.",
        "cause": "발전기 가동 상태에서 벨트 교체 시도, LOTO 미적용",
        "result": "작업자 1명 손가락 3개 절단",
        "countermeasure": "발전기 완전 정지 후 LOTO 적용 상태에서만 정비 작업, 가동 중 회전체 접근 금지 표시 강화",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2024-04", "work_keywords": "발전기가동",
        "accident_type": "화재",
        "summary": "발전기 연료 필터 교체 후 재기동 시 연결부 체결 불량으로 연료가 누출되어 화재가 발생하였다.",
        "cause": "필터 교체 후 연결부 최종 체결 확인 미실시, 누설 시험 생략",
        "result": "발전기실 화재, 초기 진화 성공, 발전기 손상",
        "countermeasure": "필터 교체 후 연결부 체결 토크 확인 및 누설 시험 의무화, 재기동 전 연료 라인 전체 점검",
        "source": "조선해양산업 안전사고 분석집",
    },
    {
        "date": "2023-12", "work_keywords": "발전기가동",
        "accident_type": "부상",
        "summary": "발전기 부하 시험 준비 중 고압 배전반 내 작업 시 아크 플래시가 발생하여 작업자가 화상을 입었다.",
        "cause": "활선 상태에서 배전반 내부 작업, 아크 플래시 보호구 미착용",
        "result": "작업자 1명 얼굴 및 팔 화상(2도), 입원 치료 1주",
        "countermeasure": "고압 배전반 작업 시 전원 차단 의무화, 아크 플래시 보호구 착용, 활선 작업 허가서 별도 발행",
        "source": "고용노동부 산업재해 현황",
    },

    # 전기작업 (6건)
    {
        "date": "2024-03", "work_keywords": "전기작업",
        "accident_type": "감전",
        "summary": "선박 내 전기 케이블 포설 작업 중 기존 활선 케이블에 접촉하여 감전되었다.",
        "cause": "케이블 트레이 내 활선 케이블 식별 미흡, 절연 장갑 미착용",
        "result": "작업자 1명 감전 경상",
        "countermeasure": "케이블 포설 전 인근 케이블 활선 여부 확인, 절연 장갑 착용 의무화, 활선 케이블 색 식별 기준 강화",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2023-09", "work_keywords": "전기작업",
        "accident_type": "화재",
        "summary": "전기 배선 작업 중 허용 전류를 초과하는 부하가 인가되어 케이블이 과열되어 화재가 발생하였다.",
        "cause": "케이블 허용 전류 용량 미검토, 과부하 보호 장치 미설치",
        "result": "케이블 트레이 화재, 인근 케이블 소손, 인명 피해 없음",
        "countermeasure": "케이블 포설 전 부하 전류 계산 및 케이블 용량 검토, 과부하 보호 장치 설치 확인",
        "source": "조선해양산업 안전사고 분석집",
    },
    {
        "date": "2024-07", "work_keywords": "전기작업",
        "accident_type": "감전",
        "summary": "배전반 내 단자 연결 작업 중 인근 활선 단자에 공구가 접촉하여 아크가 발생하고 작업자가 감전되었다.",
        "cause": "활선 부위 절연 커버 미설치, 작업 공간 협소로 공구 오접촉",
        "result": "작업자 1명 손 감전 부상, 아크로 인한 공구 손상",
        "countermeasure": "배전반 작업 시 인근 활선 부위 절연 커버 설치, 작업 전 전원 차단 가능 여부 재검토, 절연 공구 사용",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2023-11", "work_keywords": "전기작업",
        "accident_type": "감전",
        "summary": "전기 시험 중 절연 저항 측정기 오조작으로 고전압이 인가되어 접촉한 작업자가 감전되었다.",
        "cause": "계측기 사용법 미숙, 시험 전 접지 미확인",
        "result": "작업자 1명 감전 경상, 심전도 검사 실시",
        "countermeasure": "계측기 사용 전 교육 이수 의무화, 시험 전 접지 상태 확인, 동료 작업자 안전 거리 확보",
        "source": "고용노동부 산업재해 현황",
    },
    {
        "date": "2024-05", "work_keywords": "전기작업",
        "accident_type": "화재",
        "summary": "전기 패널 내부 청소 작업 중 먼지가 아크를 일으켜 화재가 발생하였다.",
        "cause": "활선 상태에서 압축 공기 청소 작업 진행, 먼지 비산으로 단락",
        "result": "전기 패널 화재, 일부 소손, 인명 피해 없음",
        "countermeasure": "전기 패널 청소 시 전원 차단 후 작업, 압축 공기 청소 시 활선 부위 절연 커버 설치",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2023-07", "work_keywords": "전기작업",
        "accident_type": "추락",
        "summary": "선박 상부 갑판 전기 케이블 트레이 설치 작업 중 사다리에서 추락하였다.",
        "cause": "사다리 고정 불량, 상단 작업 시 안전대 미착용",
        "result": "작업자 1명 추락으로 허리 골절, 입원 치료 4주",
        "countermeasure": "사다리 사용 전 고정 상태 확인, 2m 이상 높이 작업 시 안전대 착용 의무화, 고소 작업 허가서 발행",
        "source": "조선해양산업 안전사고 분석집",
    },

    # 일반작업 (4건)
    {
        "date": "2023-08", "work_keywords": "일반작업",
        "accident_type": "끼임",
        "summary": "자재 이송 작업 중 크레인 이동 경로 내에 있던 작업자가 자재와 구조물 사이에 끼였다.",
        "cause": "크레인 이동 경로 내 작업자 존재, 신호수 미배치",
        "result": "작업자 1명 흉부 압박으로 중상, 병원 이송 후 회복",
        "countermeasure": "크레인 작업 전 이동 경로 내 작업자 퇴거 확인, 신호수 배치 의무화, 작업 반경 표시 및 출입 통제",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2024-01", "work_keywords": "일반작업",
        "accident_type": "추락",
        "summary": "선체 도장 작업 중 비계 발판이 파손되어 작업자 1명이 6m 아래로 추락하였다.",
        "cause": "비계 발판 노후화로 파손, 안전대 미체결",
        "result": "작업자 1명 척추 골절 중상, 장기 입원 치료",
        "countermeasure": "비계 발판 매일 점검 및 이상 발견 시 즉시 교체, 고소 작업 시 안전대 체결 의무화, 안전 관리자 순시 강화",
        "source": "고용노동부 산업재해 현황",
    },
    {
        "date": "2023-10", "work_keywords": "일반작업",
        "accident_type": "부상",
        "summary": "자재 운반 작업 중 무거운 자재를 혼자 들다가 허리 부상을 당하였다.",
        "cause": "중량물 2인 1조 작업 기준 미준수, 보조 장비 미사용",
        "result": "작업자 1명 요추 염좌, 1주 병가",
        "countermeasure": "25kg 이상 중량물 2인 이상 작업 원칙 준수, 리프팅 장비 적극 활용, 근골격계 예방 교육 실시",
        "source": "한국산업안전보건공단 (kosha.or.kr)",
    },
    {
        "date": "2024-08", "work_keywords": "일반작업",
        "accident_type": "부상",
        "summary": "절단 작업 중 그라인더 디스크가 파손되어 파편이 비산하여 작업자 얼굴에 부상을 입혔다.",
        "cause": "마모된 디스크 계속 사용, 보안경 미착용",
        "result": "작업자 1명 얼굴 파편 부상, 경상",
        "countermeasure": "그라인더 디스크 사용 전 균열·마모 상태 확인 후 이상 시 교체, 보안경 착용 의무화",
        "source": "조선해양산업 안전사고 분석집",
    },
]


# ── PDF 생성 함수 ──────────────────────────────────────────────────────────────
def build_pdf(acc: dict) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=20*mm, rightMargin=20*mm,
        topMargin=20*mm, bottomMargin=20*mm
    )

    fn = _FONT_NAME if _FONT_NAME in pdfmetrics.getRegisteredFontNames() else "Helvetica"

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "AccTitle", fontName=fn, fontSize=14, leading=20,
        textColor=colors.HexColor("#1e2a3a"), spaceAfter=4*mm
    )
    label_style = ParagraphStyle(
        "Label", fontName=fn, fontSize=9, leading=14,
        textColor=colors.HexColor("#6b7280"), spaceBefore=3*mm
    )
    body_style = ParagraphStyle(
        "Body", fontName=fn, fontSize=10, leading=16,
        textColor=colors.HexColor("#111827")
    )
    warn_style = ParagraphStyle(
        "Warn", fontName=fn, fontSize=10, leading=16,
        textColor=colors.HexColor("#b91c1c")
    )
    source_style = ParagraphStyle(
        "Source", fontName=fn, fontSize=8, leading=12,
        textColor=colors.HexColor("#9ca3af"), spaceBefore=4*mm
    )

    story = []

    # 헤더 배너
    story.append(Paragraph(
        f"[{acc['accident_type']}] {acc['work_keywords']} 사고사례",
        title_style
    ))
    story.append(HRFlowable(width="100%", thickness=2,
                            color=colors.HexColor("#ef4444"), spaceAfter=4*mm))

    # 날짜 + 유형 테이블
    tbl = Table(
        [["사고일시", acc["date"], "사고유형", acc["accident_type"]]],
        colWidths=[25*mm, 45*mm, 25*mm, 45*mm]
    )
    tbl.setStyle(TableStyle([
        ("FONTNAME",    (0,0), (-1,-1), fn),
        ("FONTSIZE",    (0,0), (-1,-1), 9),
        ("BACKGROUND",  (0,0), (0,0),  colors.HexColor("#f3f4f6")),
        ("BACKGROUND",  (2,0), (2,0),  colors.HexColor("#f3f4f6")),
        ("TEXTCOLOR",   (0,0), (0,0),  colors.HexColor("#374151")),
        ("TEXTCOLOR",   (2,0), (2,0),  colors.HexColor("#374151")),
        ("GRID",        (0,0), (-1,-1), 0.5, colors.HexColor("#e5e7eb")),
        ("ROWBACKGROUNDS", (0,0), (-1,-1), [colors.white]),
        ("PADDING",     (0,0), (-1,-1), 4),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 4*mm))

    # 섹션별 내용
    sections = [
        ("사고 개요",   acc["summary"],        body_style),
        ("사고 원인",   acc["cause"],          warn_style),
        ("사고 결과/피해", acc["result"],       warn_style),
        ("재발 방지 대책", acc["countermeasure"], body_style),
    ]
    for label, text, style in sections:
        story.append(Paragraph(label, label_style))
        story.append(HRFlowable(width="100%", thickness=0.5,
                                color=colors.HexColor("#d1d5db"), spaceAfter=2*mm))
        story.append(Paragraph(text, style))

    story.append(Spacer(1, 4*mm))
    story.append(Paragraph(f"출처: {acc['source']}", source_style))

    doc.build(story)
    return buf.getvalue()


# ── 데이터프레임 생성 ──────────────────────────────────────────────────────────
rows = []
for i, acc in enumerate(ACCIDENTS, 1):
    acc_id = f"ACC{i:03d}"
    pdf_filename = f"accident_{acc_id}.pdf"
    rows.append({
        "id":            acc_id,
        "date":          acc["date"],
        "summary":       acc["summary"],
        "cause":         acc["cause"],
        "result":        acc["result"],
        "countermeasure": acc["countermeasure"],
        "accident_type": acc["accident_type"],
        "work_keywords": acc["work_keywords"],
        "source":        acc["source"],
        "pdf_filename":  pdf_filename,
    })

accident_df = pd.DataFrame(rows)

# 로컬 CSV 저장
accident_df.to_csv(OUT_DIR / "accident.csv", index=False, encoding="utf-8-sig")
print(f"[OK] accident.csv  ({len(accident_df)}행)")

# PDF 생성
for i, acc in enumerate(ACCIDENTS, 1):
    acc_id = f"ACC{i:03d}"
    pdf_bytes = build_pdf(acc)
    pdf_path = PDF_DIR / f"accident_{acc_id}.pdf"
    pdf_path.write_bytes(pdf_bytes)

print(f"[OK] PDF {len(ACCIDENTS)}건 생성: output/accident_pdf/")


# ── Google Drive 업로드 ────────────────────────────────────────────────────────
def get_drive_service():
    creds = Credentials.from_service_account_file(
        KEY_PATH, scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)


def upsert_file(svc, filename: str, buf: bytes, mimetype: str, folder_id: str):
    res = svc.files().list(
        q=f"name='{filename}' and '{folder_id}' in parents and trashed=false",
        fields="files(id)",
        supportsAllDrives=True, includeItemsFromAllDrives=True
    ).execute()
    for f in res.get("files", []):
        svc.files().delete(fileId=f["id"], supportsAllDrives=True).execute()
    svc.files().create(
        body={"name": filename, "parents": [folder_id]},
        media_body=MediaIoBaseUpload(io.BytesIO(buf), mimetype=mimetype),
        fields="id", supportsAllDrives=True
    ).execute()


def get_or_create_folder(svc, name: str, parent_id: str) -> str:
    res = svc.files().list(
        q=(f"name='{name}' and '{parent_id}' in parents "
           f"and mimeType='application/vnd.google-apps.folder' and trashed=false"),
        fields="files(id)",
        supportsAllDrives=True, includeItemsFromAllDrives=True
    ).execute()
    files = res.get("files", [])
    if files:
        return files[0]["id"]
    f = svc.files().create(
        body={"name": name, "mimeType": "application/vnd.google-apps.folder",
              "parents": [parent_id]},
        fields="id", supportsAllDrives=True
    ).execute()
    return f["id"]


try:
    svc = get_drive_service()

    # accident.csv 업로드
    csv_bytes = accident_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    upsert_file(svc, "accident.csv", csv_bytes, "text/csv", FOLDER_ID)
    print("[OK] accident.csv  Drive 업로드 완료")

    # accident_pdf 폴더 생성 후 PDF 업로드
    pdf_folder_id = get_or_create_folder(svc, "accident_pdf", FOLDER_ID)
    for i in range(1, len(ACCIDENTS) + 1):
        acc_id = f"ACC{i:03d}"
        pdf_path = PDF_DIR / f"accident_{acc_id}.pdf"
        upsert_file(svc, pdf_path.name, pdf_path.read_bytes(),
                    "application/pdf", pdf_folder_id)
    print(f"[OK] PDF {len(ACCIDENTS)}건  Drive 업로드 완료 (accident_pdf/)")

except Exception as e:
    print(f"\n[WARN] Drive 업로드 실패: {e}")
    print("   로컬 output/ 폴더에서 수동으로 업로드해주세요.")

print(f"\n  사고사례 : {len(accident_df)}건")
print(f"  LNG작업  : {len([a for a in ACCIDENTS if a['work_keywords']=='LNG작업'])}건")
print(f"  고압작업 : {len([a for a in ACCIDENTS if a['work_keywords']=='고압작업'])}건")
print(f"  밀폐작업 : {len([a for a in ACCIDENTS if a['work_keywords']=='밀폐작업'])}건")
print(f"  시운전   : {len([a for a in ACCIDENTS if a['work_keywords']=='시운전작업'])}건")
print(f"  발전기   : {len([a for a in ACCIDENTS if a['work_keywords']=='발전기가동'])}건")
print(f"  전기작업 : {len([a for a in ACCIDENTS if a['work_keywords']=='전기작업'])}건")
print(f"  일반작업 : {len([a for a in ACCIDENTS if a['work_keywords']=='일반작업'])}건")
