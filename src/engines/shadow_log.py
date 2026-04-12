import psycopg2, uuid, json, time, logging, os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

DB_URL = os.environ.get('DATABASE_URL', '')

def get_pg_conn():
    return psycopg2.connect(DB_URL)

def new_id():
    return str(uuid.uuid4())

class ShadowLog:
    def __init__(self, document_id, source_type='whatsapp_image'):
        self.run_id = new_id()
        self.document_id = document_id
        self.source_type = source_type
        self.start_time = time.time()
        self.line_ids = {}
        self.blockers = 0
        self.warnings = 0
        self._init_run()

    def _init_run(self):
        try:
            conn = get_pg_conn()
            cur = conn.cursor()
            cur.execute('''
                INSERT INTO receipt_processing_runs
                (id, document_id, source_type, engine_version, status)
                VALUES (%s, %s, %s, %s, %s)
            ''', (self.run_id, self.document_id, self.source_type, '1.0', 'PROCESSING'))
            conn.commit()
            conn.close()
        except Exception as e:
            logging.warning(f'ShadowLog init failed: {e}')

    def log_line(self, line_index, raw_text, normalized_text, line_type,
                 bbox=None, confidence=None, line_height=None, parent_line_id=None, metadata=None):
        line_id = new_id()
        try:
            conn = get_pg_conn()
            cur = conn.cursor()
            cur.execute('''
                INSERT INTO receipt_lines
                (id, processing_run_id, line_index, raw_text, normalized_text,
                 line_type, bbox, avg_confidence, line_height, parent_line_id, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                line_id, self.run_id, line_index,
                raw_text, normalized_text, line_type,
                json.dumps(bbox or {}), confidence, line_height,
                parent_line_id, json.dumps(metadata or {})
            ))
            conn.commit()
            conn.close()
            self.line_ids[line_index] = line_id
        except Exception as e:
            logging.warning(f'ShadowLog log_line failed: {e}')
        return line_id

    def log_hypothesis(self, line_id, hypothesis_type, total_price,
                       quantity=None, unit_price=None, weight=None,
                       discount_amount=None, tax_code=None, gl_account=None,
                       composite_score=None, selected=False, evidence=None):
        try:
            conn = get_pg_conn()
            cur = conn.cursor()
            cur.execute('''
                INSERT INTO line_hypotheses
                (id, processing_run_id, line_id, hypothesis_type,
                 quantity, unit_price, weight, discount_amount,
                 total_price, tax_code_guess, gl_account_guess,
                 composite_score, selected, evidence)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ''', (
                new_id(), self.run_id, line_id, hypothesis_type,
                quantity, unit_price, weight, discount_amount,
                total_price, tax_code, gl_account,
                composite_score, selected, json.dumps(evidence or {})
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logging.warning(f'ShadowLog log_hypothesis failed: {e}')

    def log_event(self, event_code, message, event_type='INFO',
                  severity=5, line_id=None, details=None):
        if event_type == 'BLOCKER':
            self.blockers += 1
        elif event_type == 'WARNING':
            self.warnings += 1
        try:
            conn = get_pg_conn()
            cur = conn.cursor()
            cur.execute('''
                INSERT INTO processing_events
                (id, processing_run_id, line_id, event_type, event_code,
                 severity, message, details)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ''', (
                new_id(), self.run_id, line_id, event_type,
                event_code, severity, message, json.dumps(details or {})
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logging.warning(f'ShadowLog log_event failed: {e}')

    def log_final_item(self, description, total_price, source_line_id=None,
                       quantity=None, unit_price=None, discount_amount=None,
                       tax_code=None, gl_account=None, confidence=None,
                       requires_review=False):
        item_id = new_id()
        try:
            conn = get_pg_conn()
            cur = conn.cursor()
            cur.execute('''
                INSERT INTO final_extracted_items
                (id, processing_run_id, source_line_id, description,
                 quantity, unit_price, total_price, discount_amount,
                 tax_code, gl_account_guess, confidence, requires_review)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ''', (
                item_id, self.run_id, source_line_id, description,
                quantity, unit_price, total_price, discount_amount,
                tax_code, gl_account, confidence, requires_review
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logging.warning(f'ShadowLog log_final_item failed: {e}')
        return item_id

    def finalize(self, status, total_from_receipt=None, reconciliation_delta=None,
                 overall_confidence=None, merchant_name=None):
        processing_ms = int((time.time() - self.start_time) * 1000)
        try:
            conn = get_pg_conn()
            cur = conn.cursor()
            cur.execute('''
                UPDATE receipt_processing_runs SET
                    status = %s,
                    total_from_receipt = %s,
                    reconciliation_delta = %s,
                    overall_confidence = %s,
                    merchant_name_guess = %s,
                    blocker_count = %s,
                    warning_count = %s,
                    processing_ms = %s
                WHERE id = %s
            ''', (
                status, total_from_receipt, reconciliation_delta,
                overall_confidence, merchant_name,
                self.blockers, self.warnings, processing_ms, self.run_id
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logging.warning(f'ShadowLog finalize failed: {e}')
