import argparse
import json
import logging
import re
import emoji
from flashtext import KeywordProcessor

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions, WorkerOptions
)
from apache_beam.transforms.window import FixedWindows

logging.basicConfig(level=logging.INFO)


# ==========================================
# BƯỚC 1: THÔNG TIN CẤU HÌNH
# ==========================================
PROJECT_ID = "bigdataproject-297"
REGION = "asia-southeast1"
DATASET_NAME = "bd_silver_dataset"

SUBSCRIPTION_NAME = "bd_gcs-file-events-sub"
SUBSCRIPTION_PATH = f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_NAME}"

TABLE_ALL_COMMENTS = f"{PROJECT_ID}:{DATASET_NAME}.all_comments_sentiment"
TABLE_PERSON_SENTIMENT = f"{PROJECT_ID}:{DATASET_NAME}.person_sentiment"

TEMP_LOCATION = "gs://bd-raw-data-bronze/temp/"

BIGQUERY_SCHEMA = {
    'fields': [
        {'name': 'comment_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'video_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'channel_title', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'video_title', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'program_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'raw_text', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'cleaned_text', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'has_person', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
        {'name': 'normalized_persons', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'target_person', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'sentiment_label', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'sentiment_pos_prob', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'sentiment_neg_prob', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'sentiment_neu_prob', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'comment_published_at', 'type': 'STRING', 'mode': 'NULLABLE'}
    ]
}


# ==========================================
# BƯỚC 2: ĐỌC TÍN HIỆU PUB/SUB LẤY ĐƯỜNG DẪN FILE GCS
# ==========================================
class ParseGCSNotification(beam.DoFn):
    """Parse tin nhắn Pub/Sub để trích xuất đường dẫn file vừa upload lên GCS."""
    def process(self, element):
        try:
            msg = json.loads(element.decode('utf-8'))
            gcs_path = f"gs://{msg['bucket']}/{msg['name']}"
            logging.info(f"==> Đã nhận sự kiện file mới từ Pub/Sub: {gcs_path}")
            yield gcs_path
        except Exception as e:
            logging.error(f"Lỗi parse PubSub message: {e}")


# ==========================================
# BƯỚC 3: ĐỌC ĐƯỜNG DẪN FILE VÀ LẤY DỮ LIỆU
# ==========================================
class ReadFileContentFn(beam.DoFn):
    """Mở đường dẫn file GCS và đọc từng dòng dữ liệu JSONL."""
    def process(self, gcs_path):
        import apache_beam.io.gcp.gcsio as gcsio
        try:
            gcs = gcsio.GcsIO()
            with gcs.open(gcs_path, 'r') as f:
                for line in f:
                    line_str = line.decode('utf-8').strip()
                    if line_str:
                        record = json.loads(line_str)
                        record['_file_path'] = gcs_path
                        yield record
        except Exception as e:
            logging.error(f"Lỗi đọc nội dung file GCS ({gcs_path}): {e}")


# ==========================================
# BƯỚC 4: HÀM XỬ LÝ DỮ LIỆU (DATA PROCESSING)
# ==========================================
class RouteRecordFn(beam.DoFn):
    """Phân loại bản ghi là Video Snapshot hoặc Comment."""
    def process(self, record):
        file_path = record.get('_file_path', '')
        if 'video_snapshots' in file_path or 'channel_title' in record:
            yield beam.pvalue.TaggedOutput('videos', (record.get('video_id'), record))
        elif 'comments' in file_path or 'comment_id' in record:
            yield beam.pvalue.TaggedOutput('comments', (record.get('video_id'), record))


class CleanTextFn(beam.DoFn):
    """Làm sạch văn bản thô."""
    def process(self, record):
        text = record.get('raw_text', '')
        if not text:
            yield record
            return
        text = emoji.demojize(text)
        text = re.sub(r'http[s]?://\S+', ' ', text)
        text = re.sub(r'[^\w\s\.,!?@_:-]', ' ', text)
        text = re.sub(r'(.)\1{2,}', r'\1', text)
        text = re.sub(r'\s+', ' ', text).strip()
        record['cleaned_text'] = text
        yield record


def extract_program_name(video_title, channel_title=None, existing_program=None):
    title = (str(video_title) or "").lower()
    channel = (str(channel_title) or "").lower()

    if any(kw in channel for kw in ["đông tây", "dong tay"]):
        if any(kw in title for kw in ["2n1d", "2 ngày 1 đêm"]): return "2 Ngày 1 Đêm"
        if "hành trình rực rỡ" in title: return "Hành Trình Rực Rỡ"
        if "tổ đội" in title: return "Tổ Đội 1 Không 2"
        if "la cà thú vị" in title: return "La Cà Thú Vị"
        if "7 nụ" in title or "bảy nụ" in title: return "7 Nụ Cười Xuân"
        if "nhanh như chớp" in title: return "Nhanh Như Chớp"
        if "kỳ tài thách đấu" in title: return "Kỳ Tài Thách Đấu"
        if "BTS ATVCDN" in title: return "Anh Trai Và Cái Đuôi Nhỏ"
        if "Anh Trai Và Cái Đuôi Nhỏ" in title: return "Anh Trai Và Cái Đuôi Nhỏ"

    elif "forest" in channel:
        if any(kw in title for kw in ["xuân hạ thu đông", "xhtdrlx", "xhtđrlx"]): return "Xuân Hạ Thu Đông Rồi Lại Xuân"
        if any(kw in title for kw in ["running man", "RUN MOMENT", "BTS", "chơi là chạy"]): return "Running Man Vietnam"
        if "mùi vị những chuyến đi" in title: return "Mùi Vị Những Chuyến Đi"
        if "eye contact live" in title: return "Eye Contact Live"

    elif "vie" in channel:
        if any(kw in title for kw in ["say hi", "anh trai", "say real", "anh trai say hi"]):
            return "Anh Trai Say Hi"
        if any(kw in title for kw in ["rap việt", "rap viet"]): return "Rap Việt"
        if any(kw in title for kw in ["The Masked Singer", "ca sĩ mặt nạ"]): return "Ca Sĩ Mặt Nạ"
        if any(kw in title for kw in ["người ấy là ai", "nala"]): return "Người Ấy Là Ai"
        if "siêu trí tuệ" in title: return "Siêu Trí Tuệ Việt Nam"
        if any(kw in title for kw in ["THSH", "Tinh Hà Say Hi", "TINH HÀ SAY HI", "UNCUT"]):
          return "Tinh Hà Say Hi"

    elif "yeah1" in channel:
        if any(kw in title for kw in ["anh trai vượt ngàn chông gai", "atvncg", "chông gai"]):
            return "Anh Trai Vượt Ngàn Chông Gai"
        if any(kw in title for kw in ["chị đẹp đạp gió", "chị đẹp rẽ sóng", "chị đẹp"]):
            return "Chị Đẹp Đạp Gió Rẽ Sóng"
        if "gia đình haha" in title: return "Gia Đình Haha"
        if "Qua Nhà Bật Nhạc" in title: return "Qua Nhà Bật Nhạc"

    if any(kw in title for kw in ["2n1d", "2 ngày 1 đêm"]): return "2 Ngày 1 Đêm"
    if any(kw in title for kw in ["say hi", "say real", "tinh hà", "anh trai say hi"]): return "Anh Trai Say Hi"
    if any(kw in title for kw in ["rap việt", "rap viet"]): return "Rap Việt"
    if any(kw in title for kw in ["the masked singer", "ca sĩ mặt nạ"]): return "Ca Sĩ Mặt Nạ"
    if any(kw in title for kw in ["chông gai", "atvncg", "anh trai vượt ngàn"]): return "Anh Trai Vượt Ngàn Chông Gai"
    if any(kw in title for kw in ["chị đẹp đạp gió", "chị đẹp"]): return "Chị Đẹp Đạp Gió Rẽ Sóng"
    if "running man" in title: return "Running Man Vietnam"

    if existing_program and str(existing_program).strip().lower() not in ("none", "null", "", "nan"):
        return str(existing_program).strip()

    return "Chưa xác định"


class AddProgramNameFn(beam.DoFn):
    def process(self, record):
        record["program_name"] = extract_program_name(
            video_title=record.get("video_title", ""),
            channel_title=record.get("channel_title", ""),
            existing_program=record.get("program_name")
        )
        yield record


def join_video_and_comments(element):
    video_id, grouped_data = element
    videos = grouped_data['videos']
    comments = grouped_data['comments']
    if not comments:
        return
    video_info = videos[0] if videos else {}
    channel_title = video_info.get('channel_title', 'Unknown')
    video_title = video_info.get('video_title', 'Unknown')
    for comment in comments:
        comment['channel_title'] = channel_title
        comment['video_title'] = video_title
        yield comment


class NormalizeEntityFn(beam.DoFn):
    """Chuẩn hóa tên nghệ sĩ theo từ điển bí danh[cite: 2]."""
    def setup(self):
        self.keyword_processor = KeywordProcessor(case_sensitive=False)
        alias_dict = {
            "Trường Giang": ["trường giang", "truong giang", "mười khó", "muoi kho", "tg", "anh giang", "giang ca"],
            "Hiếu thứ 2": ["hieuthuhai", "hieu thu hai", "hiếu thứ hai", "hiếu thứ 2", "hieuthu2", "hth", "atsh hieuthuhai", "chủ tịch hieu"],
            "Lê Dương Bảo Lâm": ["lê dương bảo lâm", "le duong bao lam", "dương lâm", "duong lam", "lee min ho đồng nai", "lee min ho dong nai", "ldbl", "lê dương", "le duong", "dương lâm đồng nai"],
            "Kiều Minh Tuấn": ["kiều minh tuấn", "kieu minh tuan", "kmt", "anh tuấn", "anh tuan", "tuấn sạp"],
            "Ngô Kiến Huy": ["ngô kiến huy", "ngo kien huy", "nkh", "chàng bắp", "chang bap", "bắp", "bap"],
            "Cris Phan": ["cris phan", "crisphan", "cris", "cris devil gamer", "phan vũ thanh cris"],
            "Song Luân": ["song luân", "song luan", "songluan"],
            "Cody Nam Võ": ["cody nam võ", "cody nam vo", "cody", "nam võ", "nam vo", "cody namvo"],
            "Võ Tấn Phát": ["võ tấn phát", "vo tan phat", "vtp", "tấn phát", "tan phat"],
            "Quang Hùng MasterD": ["quang hùng masterd", "quang hung masterd", "quanghungmasterd", "quang hùng", "quang hung", "qhm", "masterd", "bé lười"],
            "CONGB": ["congb", "công b", "cong b", "côngb", "congb official"],
            "JSOL": ["jsol", "j sol", "j-sol", "thái sơn"],
            "Wren Evans": ["wren evans", "wren evan", "wren", "wrenevans", "lê phan"],
            "Đặng Hồng Hải": ["đặng hồng hải", "dang hong hai", "đặng hải", "dang hai", "đhh", "dhh", "hồng hải", "hong hai"],
            "Dương Domic": ["dương domic", "duong domic", "duongdomic", "d domic", "trần đăng dương"],
            "HURRYKNG": ["hurrykng", "hurry kng", "hurrykngg", "khang", "phạm bảo khang"],
            "WEAN": ["wean", "wean le", "wean lê", "weanle", "Lê Thế Anh"],
            "Sơn.K": ["sơn.k", "son.k", "sơn k", "son k", "sơnk", "sonk"],
            "Xuân Định K.Y": ["xuân định k.y", "xuan dinh k.y", "xuân định ky", "xuan dinh ky", "xuân định", "xuan dinh", "xdk"],
            "Pháp Kiều": ["pháp kiều", "phap kieu", "phapkieu", "nguyễn pháp kiều"],
            "CoolKid": ["coolkid", "cool kid", "cool-kid"],
            "Bùi Trường Linh": ["bùi trường linh", "bui truong linh", "buitruonglinh", "btl"],
            "Captain Boy": ["captain boy", "captainboy", "captain", "cap boy", "hoàng thế nguyên"],
            "Jaysonlei": ["jaysonlei", "jayson lei"],
            "Saabirose": ["saabirose", "saabi rose"],
            "HYO": ["hyo", "hyo official"],
            "Vương Bình": ["vương bình", "vuong binh"],
            "Hùng Huỳnh": ["hùng huỳnh", "hung huynh", "hunghuynh"],
            "Rhyder": ["rhyder", "quang anh", "nguyễn quang anh"],
            "Negav": ["negav", "đặng thành an"],
            "Ali Hoàng Dương": ["ali hoàng dương", "ali hoang duong"],
            "Isaac": ["isaac", "isaac 365", "phạm lưu tuấn tài"],
            "Phương Mỹ Chi": ["phương mỹ chi", "phuong my chi", "pmc", "cô bé dân ca"],
            "Đức Phúc": ["đức phúc", "duc phuc"],
            "Bích Phương": ["bích phương", "bich phuong"],
            "Trấn Thành": ["trấn thành", "tran thanh", "a xìn", "a xin", "mc tran thanh", "huỳnh trấn thành"],
            "Pháo": ["pháo", "phao", "pháo northside", "nguyễn diệu huyền"],
            "Hữu Đằng": ["hữu đằng", "huu dang"],
            "Ánh Sáng": ["ánh sáng", "anh sang"],
            "Yeolan": ["yeolan", "yeo lan"],
            "Bùi Duy Ngọc": ["bùi duy ngọc", "bui duy ngoc"],
            "Hari Won": ["hari won", "hariwon"],
            "Sơn Tùng M-TP": ["sơn tùng", "son tung", "m-tp", "sơn tùng m-tp", "thanh đồng", "nguyễn thanh tùng"],
            "Jack": ["jack", "trịnh phương tuấn", "j97"],
            "Mỹ Tâm": ["mỹ tâm", "my tam", "họa mi tóc nâu"],
            "Hồ Ngọc Hà": ["hồ ngọc hà", "ho ngoc ha"],
            "Đen Vâu": ["đen vâu", "den vau", "đen", "nguyễn đức cường"],
            "Binz": ["binz", "bigcityboi", "lê nguyễn trung đan"],
            "Suboi": ["suboi", "hàng lâm ánh ngọc"],
            "Karik": ["karik", "phạm hoàng khoa"],
            "Wowy": ["wowy", "nguyễn văn huy"],
            "JustaTee": ["justatee", "thanh tuấn", "nguyễn thanh tuấn"],
            "B Ray": ["b ray", "bray", "trần thiện thanh bảo"],
            "Rhymastic": ["rhymastic", "vũ đức thiện"],
            "Tóc Tiên": ["tóc tiên", "toc tien", "nguyễn khoa tóc tiên"],
            "Hoàng Thùy Linh": ["hoàng thùy linh", "hoang thuy linh"],
            "Chi Pu": ["chi pu", "nguyễn thuỳ chi"],
            "Soobin Hoàng Sơn": ["soobin", "soobin hoàng sơn", "nguyễn hoàng sơn"],
            "Erik": ["erik", "lê trung thành"],
            "Hòa Minzy": ["hòa minzy", "hoa minzy", "nguyễn thị hòa"],
            "Amee": ["amee", "trần huyền mỹ"],
            "Monstera": ["monstera"],
            "Hai Thành": ["hai thành", "hai thanh"],
            "Tám Cúc": ["tám cúc", "tam cuc"],
            "Sáu Tút": ["sáu tút", "sau tut"],
            "Tư Ngọc": ["tư ngọc", "tu ngoc"],
        }
        self.keyword_processor.add_keywords_from_dict(alias_dict)

    def process(self, record):
        persons = record.get("extracted_persons", []) or []
        text = record.get("cleaned_text", "") or ""
        title = record.get("video_title", "") or ""

        normalized = set()

        for person in persons:
            person = str(person).strip()
            if not person:
                continue
            matches = self.keyword_processor.extract_keywords(person)
            if matches:
                normalized.update(matches)

        text_matches_with_spans = self.keyword_processor.extract_keywords(text, span_info=True)
        normalized.update(match[0] for match in text_matches_with_spans)
        record["person_mentions"] = [
            {"person": match[0], "start": match[1], "end": match[2]}
            for match in text_matches_with_spans
        ]

        title_lower = title.lower()
        text_lower = text.lower()

        if "hieuthuhai" in title_lower or "hiếu thứ hai" in title_lower or "hieu thu hai" in title_lower:
            if any(alias in text_lower for alias in ["hiếu", "hieu", "thằng hiếu", "thang hieu"]):
                normalized.add("Hiếu thứ 2")

        if "2n1d" in title_lower or "2 ngày 1 đêm" in title_lower:
            initials = {
                "kmt": "Kiều Minh Tuấn",
                "nkh": "Ngô Kiến Huy",
                "vtp": "Võ Tấn Phát",
                "ldbl": "Lê Dương Bảo Lâm",
            }
            for short_name, full_name in initials.items():
                if short_name in text_lower.split():
                    normalized.add(full_name)

        if (
            any(k in title_lower for k in ["hieuthuhai", "hiếu thứ hai", "hieu thu hai"])
            and re.search(r"\bhiếu\b|\bhieu\b", text_lower)
        ):
            normalized.add("Hiếu thứ 2")

        record["normalized_persons"] = sorted(list(normalized), key=str.casefold)
        record["has_person"] = len(normalized) > 0
        yield record


class ExpandPersonRowsFn(beam.DoFn):
    """Tách dòng văn bản theo từng nhân vật được nhắc đến[cite: 2]."""
    def process(self, record):
        text = record.get("cleaned_text", "") or ""
        persons = record.get("normalized_persons", []) or []
        mentions = record.get("person_mentions", []) or []

        if not text or not persons:
            return

        split_pattern = (
            r"(?<=[.!?;])\s+"
            r"|\s+(?i:nhưng|trong khi|còn|"
            r"tuy nhiên|ngược lại|whereas|but)\s+"
        )

        clauses = []
        last_end = 0

        for match in re.finditer(split_pattern, text):
            start, end = last_end, match.start()
            clause = text[start:end].strip()

            if clause:
                actual_start = text.find(clause, start, end)
                clauses.append((actual_start, actual_start + len(clause), clause))

            last_end = match.end()

        tail = text[last_end:].strip()
        if tail:
            actual_start = text.find(tail, last_end)
            clauses.append((actual_start, actual_start + len(tail), tail))

        if not clauses:
            clauses = [(0, len(text), text)]

        person_contexts = {}

        for person in sorted(set(persons), key=str.casefold):
            person_mentions = [m for m in mentions if m.get("person") == person]
            contexts = []

            for mention in person_mentions:
                mention_start = mention.get("start", -1)
                mention_end = mention.get("end", -1)

                for start, end, clause in clauses:
                    if start <= mention_start and mention_end <= end:
                        contexts.append(clause)
                        break

            contexts = list(dict.fromkeys(contexts))

            if not contexts:
                contexts = [text]

            person_contexts[person] = contexts

        for person, contexts in person_contexts.items():
            for context in contexts:
                row = dict(record)
                row["target_person"] = person
                row["normalized_persons"] = [person]
                row["cleaned_text"] = context
                yield row


# ==========================================
# BƯỚC 5: HÀM XỬ LÝ NGÔN NGỮ LỚN (AI / NLP MODELS)
# ==========================================
class ExtractPersonNERFn(beam.DoFn):
    def setup(self):
        import torch
        from transformers import pipeline
        self.ner_pipeline = pipeline(
            "ner",
            model="NlpHUST/ner-vietnamese-electra-base",
            aggregation_strategy="simple",
            device=0 if torch.cuda.is_available() else -1,
    )

    def process(self, record):
        text = record.get('cleaned_text', '')
        if text:
            try:
                entities = self.ner_pipeline(text)
                persons = [ent['word'] for ent in entities if 'PER' in ent['entity_group']]
                record['extracted_persons'] = list(set(persons))
                record['has_person'] = len(persons) > 0
            except Exception:
                record['extracted_persons'] = []
                record['has_person'] = False
        else:
            record['extracted_persons'] = []
            record['has_person'] = False
        yield record


class SentimentAnalysisFn(beam.DoFn):
    """Sử dụng mô hình PhoBERT để phân tích cảm xúc văn bản[cite: 2]."""
    def setup(self):
        import torch
        from transformers import AutoTokenizer, AutoModelForSequenceClassification
        self.torch = torch
        model_name = "wonrax/phobert-base-vietnamese-sentiment"
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name).to(self.device)
        self.model.eval()

    def process(self, record):
        text = record.get('cleaned_text', '')
        if text:
            try:
                inputs = self.tokenizer(text, return_tensors="pt", truncation=True, max_length=256).to(self.device)
                with self.torch.no_grad(): 
                    outputs = self.model(**inputs)
                probs = self.torch.softmax(outputs.logits, dim=1).squeeze().tolist()
                record['sentiment_neg_prob'] = round(probs[0], 4)
                record['sentiment_pos_prob'] = round(probs[1], 4)
                record['sentiment_neu_prob'] = round(probs[2], 4)
                labels = ['NEGATIVE', 'POSITIVE', 'NEUTRAL']
                record['sentiment_label'] = labels[probs.index(max(probs))]
            except Exception:
                record['sentiment_label'] = 'ERROR'
        else:
            record['sentiment_label'] = 'EMPTY'

        record['normalized_persons_str'] = ", ".join(record.get('normalized_persons', []))

        yield {
            "comment_id": record.get("comment_id"),
            "video_id": record.get("video_id"),
            "channel_title": record.get("channel_title"),
            "video_title": record.get("video_title"),
            "program_name": record.get("program_name", "Chưa xác định"),
            "raw_text": record.get("raw_text"),
            "cleaned_text": record.get("cleaned_text"),
            "has_person": record.get("has_person"),
            "normalized_persons": record.get("normalized_persons_str"),
            "target_person": record.get("target_person", None),
            "sentiment_label": record.get("sentiment_label"),
            "sentiment_pos_prob": record.get("sentiment_pos_prob"),
            "sentiment_neg_prob": record.get("sentiment_neg_prob"),
            "sentiment_neu_prob": record.get("sentiment_neu_prob"),
            "comment_published_at": record.get("comment_published_at")
        }


# ==========================================
# BƯỚC 6 & 7: PIPELINE TOÀN BỘ VÀ ĐẨY LÊN BIGQUERY (APPEND)
# ==========================================
def run():
    options = PipelineOptions()

    gco = options.view_as(GoogleCloudOptions)
    gco.project = PROJECT_ID
    gco.region = REGION
    gco.temp_location = TEMP_LOCATION

    options.view_as(StandardOptions).runner = "DataflowRunner"
    options.view_as(StandardOptions).streaming = True

    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True

    worker = options.view_as(WorkerOptions)
    worker.sdk_container_image = f"{REGION}-docker.pkg.dev/{PROJECT_ID}/REPO/nlp-beam:v1"
    worker.machine_type = "n2-standard-4"
    #worker.number_of_worker_harness_threads = 2

    with beam.Pipeline(options=options) as p:
        # BƯỚC 2: Nhận tín hiệu file từ Pub/Sub
        pubsub_messages = (
            p 
            | "Step 2 - Read PubSub" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION_PATH)
            | "Step 2 - Parse Notification" >> beam.ParDo(ParseGCSNotification())
            | "Step 3 - Read File Content" >> beam.ParDo(ReadFileContentFn())
        )

        # BƯỚC 4: Phân loại dữ liệu (Videos vs Comments)
        routed_data = (
            pubsub_messages 
            | "Step 4 - Route Records" >> beam.ParDo(RouteRecordFn()).with_outputs('videos', 'comments')
        )

        # Tạo cửa sổ thời gian (Windowing) phục vụ Join trong môi trường Streaming
        videos_windowed = (
            routed_data.videos 
            | "Step 4 - Window Videos" >> beam.WindowInto(FixedWindows(300))
        )

        comments_windowed = (
            routed_data.comments 
            | "Step 4 - Window Comments" >> beam.WindowInto(FixedWindows(300))
        )

        # Ghép nối video_info vào comment, làm sạch văn bản và bổ sung tên chương trình
        joined = (
            {'videos': videos_windowed, 'comments': comments_windowed}
            | "Step 4 - Join Stream Data" >> beam.CoGroupByKey()
            | "Step 4 - Merge Video Info" >> beam.FlatMap(join_video_and_comments)
            | "Step 4 - Clean Text" >> beam.ParDo(CleanTextFn())
            | "Step 4 - Extract Program" >> beam.ParDo(AddProgramNameFn())
        )

        # LOGIC KHỬ TRÙNG LẶP DỮ LIỆU (DEDUPLICATION):
        # Gom nhóm theo comment_id để loại bỏ trùng lặp nếu nhận lại tin nhắn PubSub hoặc quét trùng file[cite: 2]
        unique_comments = (
            joined
            | "Step 4 - Key By Comment ID" >> beam.Map(lambda record: (record.get("comment_id"), record))
            | "Step 4 - Group By Comment ID" >> beam.GroupByKey()
            | "Step 4 - Deduplicate" >> beam.Map(lambda kv: kv[1][0])
        )

        # ----------------------------------------------------
        # BẢNG 1: ALL_COMMENTS_SENTIMENT (Append - Chỉ thêm mới)[cite: 2]
        # ----------------------------------------------------
        _ = (
            unique_comments
            | "Step 4 - Normalize Names All" >> beam.ParDo(NormalizeEntityFn())
            | "Step 5 - Sentiment Model All" >> beam.ParDo(SentimentAnalysisFn())
            | "Step 6 - Append BQ All Comments" >> beam.io.WriteToBigQuery(
                table=TABLE_ALL_COMMENTS,
                schema=BIGQUERY_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=TEMP_LOCATION
            )
        )

        # ----------------------------------------------------
        # BẢNG 2: PERSON_SENTIMENT (Append - Chỉ thêm mới)[cite: 2]
        # ----------------------------------------------------
        _ = (
            unique_comments
            | "Step 5 - NER Model Person" >> beam.ParDo(ExtractPersonNERFn())
            | "Step 4 - Normalize Names Person" >> beam.ParDo(NormalizeEntityFn())
            | "Step 4 - Filter Has Person" >> beam.Filter(lambda x: x.get('has_person', False))
            | "Step 4 - Expand Persons" >> beam.ParDo(ExpandPersonRowsFn())
            | "Step 5 - Sentiment Model Person" >> beam.ParDo(SentimentAnalysisFn())
            | "Step 6 - Append BQ Person Sentiment" >> beam.io.WriteToBigQuery(
                table=TABLE_PERSON_SENTIMENT,
                schema=BIGQUERY_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=TEMP_LOCATION
            )
        )


if __name__ == '__main__':
    logging.info("Khởi chạy Pipeline xử lý dữ liệu streaming thời gian thực...")
    run()