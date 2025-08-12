"""
Amazon Aurora DSQL Database Connector - STUDY IMPLEMENTATION SKELETON
Participants will implement this connector to integrate with Aurora DSQL

This file contains TODO items that participants need to complete during the study.
"""

import logging
import os
import re
from typing import Any, Dict, List, Optional
import boto3
import psycopg2
import psycopg2.extras
import time
from contextlib import closing

from .base_connector import BaseDatabaseConnector

logger = logging.getLogger(__name__)


class AuroraDSQLConnector(BaseDatabaseConnector):

    def get_inventory_paginated(
        self,
        warehouse_id: Optional[int] = None,
        threshold: Optional[int] = 10,
        search: Optional[str] = None,
        limit: int = 100,
        page: int = 1,
    ) -> Dict[str, Any]:
        """
        Return inventory rows (ITEM \u00d7 STOCK) with pagination and optional filters.
        This signature matches InventoryService expectations.

        Returns a dict:
        {
            "inventory": List[dict],
            "total_count": int,
            "limit": int,
            "page": int,
            "has_next": bool,
            "has_prev": bool,
        }
        """
        self._ensure_connection()

        # Sanitize paging
        page = max(int(page or 1), 1)
        limit = max(int(limit or 100), 1)
        offset = (page - 1) * limit

        # Build filters
        where = ["1=1"]
        params_total: list = []
        params_page: list = []

        if warehouse_id is not None:
            where.append("s.s_w_id = %s")
            params_total.append(warehouse_id)
            params_page.append(warehouse_id)

        if isinstance(threshold, int) and threshold is not None and threshold > 0:
            where.append("s.s_quantity < %s")
            params_total.append(threshold)
            params_page.append(threshold)

        if search:
            
            where.append("i.i_name ILIKE %s")
            like = f"%{search}%"
            params_total.append(like)
            params_page.append(like)

        where_sql = " AND ".join(where)
        no_filters = (
            (warehouse_id is None) and (not search) and (not (isinstance(threshold, int) and threshold > 0))
        )

        # Total count (try fast estimate when completely unfiltered)
        if no_filters:
            # inventory 总量 ~= stock 行数（每个仓库×商品一行），以 stock 为准
            total_count = self._approx_rowcount('stock')
        else:
            total_sql = (
                "SELECT COUNT(*) AS cnt "
                "FROM item i JOIN stock s ON s.s_i_id = i.i_id "
                f"WHERE {where_sql}"
            )
            total_rows = self.execute_query(total_sql, tuple(params_total) if params_total else None)
            total_count = int(total_rows[0]["cnt"]) if total_rows else 0

        # Page query
        data_sql = (
            "SELECT i.i_id, i.i_name, i.i_price, "
            "       s.s_w_id AS w_id, s.s_quantity, s.s_ytd, s.s_order_cnt "
            "FROM item i JOIN stock s ON s.s_i_id = i.i_id "
            f"WHERE {where_sql} "
            "ORDER BY i.i_id ASC "
            "LIMIT %s OFFSET %s"
        )
        params_page = list(params_page) + [limit, offset]
        rows: List[Dict[str, Any]] = []
        try:
            rows = self.execute_query(data_sql, tuple(params_page))
        except Exception as e:
            msg = str(e).lower()
            # Fallback: avoid expensive global sort when temp space is exceeded
            if "temp space" in msg or "temporary file" in msg or "out of memory" in msg:
                logger.warning(
                    "Falling back to no-order pagination for inventory due to temp space limits: %s",
                    e,
                )
                # Remove ORDER BY to avoid large sort; rely on natural order
                data_sql_no_order = (
                    "SELECT i.i_id, i.i_name, i.i_price, "
                    "       s.s_w_id AS w_id, s.s_quantity, s.s_ytd, s.s_order_cnt "
                    "FROM item i JOIN stock s ON s.s_i_id = i.i_id "
                    f"WHERE {where_sql} "
                    "LIMIT %s OFFSET %s"
                )
                rows = self.execute_query(data_sql_no_order, tuple(params_page))
            else:
                raise

        has_prev = page > 1
        has_next = (page * limit) < total_count

        return {
            "inventory": rows,
            "total_count": total_count,
            "limit": limit,
            "page": page,
            "has_next": has_next,
            "has_prev": has_prev,
        }

    def get_payment_history_paginated(
        self,
        warehouse_id: Optional[int] = None,
        district_id: Optional[int] = None,
        customer_id: Optional[int] = None,
        limit: int = 50,
        page: int = 1,
    ) -> Dict[str, Any]:
        """
        Return payment HISTORY with pagination and optional filters.
        Returns a dict: {"items": List[dict], "total": int, "limit": int, "page": int}
        """
        self._ensure_connection()

        page = max(int(page or 1), 1)
        limit = max(int(limit or 50), 1)
        offset = (page - 1) * limit

        where = ["1=1"]
        params_total: list = []
        params_page: list = []

        if warehouse_id is not None:
            where.append("h.h_w_id = %s")
            params_total.append(warehouse_id)
            params_page.append(warehouse_id)
        if district_id is not None:
            where.append("h.h_d_id = %s")
            params_total.append(district_id)
            params_page.append(district_id)
        if customer_id is not None:
            where.append("h.h_c_id = %s")
            params_total.append(customer_id)
            params_page.append(customer_id)

        where_sql = " AND ".join(where)
        no_filters = (warehouse_id is None and district_id is None and customer_id is None)

        if no_filters:
            total = self._approx_rowcount('history')
        else:
            total_sql = (
                "SELECT COUNT(*) AS cnt FROM history h "
                f"WHERE {where_sql}"
            )
            total_rows = self.execute_query(total_sql, tuple(params_total) if params_total else None)
            total = int(total_rows[0]["cnt"]) if total_rows else 0

        data_sql = (
            "SELECT h.h_w_id AS w_id, h.h_d_id AS d_id, h.h_c_id AS c_id, "
            "       h.h_date, h.h_amount, h.h_data, "
            "       c.c_first, c.c_middle, c.c_last "
            "FROM history h "
            "LEFT JOIN customer c ON c.c_w_id = h.h_c_w_id AND c.c_d_id = h.h_c_d_id AND c.c_id = h.h_c_id "
            f"WHERE {where_sql} "
            "LIMIT %s OFFSET %s"
        )
        params_page = list(params_page) + [limit, offset]
        rows: List[Dict[str, Any]] = []
        try:
            rows = self.execute_query(data_sql, tuple(params_page))
        except Exception as e:
            msg = str(e).lower()
            if "temp space" in msg or "temporary file" in msg or "out of memory" in msg:
                logger.warning(
                    "Falling back to no-order pagination for payment history due to temp space limits: %s",
                    e,
                )
                data_sql_no_order = (
                    "SELECT h.h_w_id AS w_id, h.h_d_id AS d_id, h.h_c_id AS c_id, "
                    "       h.h_date, h.h_amount, h.h_data, "
                    "       c.c_first, c.c_middle, c.c_last "
                    "FROM history h "
                    "LEFT JOIN customer c ON c.c_w_id = h.h_c_w_id AND c.c_d_id = h.h_c_d_id AND c.c_id = h.h_c_id "
                    f"WHERE {where_sql} "
                    "LIMIT %s OFFSET %s"
                )
                rows = self.execute_query(data_sql_no_order, tuple(params_page))
            else:
                raise

        return {"items": rows, "total": total, "limit": limit, "page": page}
    """
    Amazon Aurora DSQL database connector for TPC-C application

    Participants will implement connection management and query execution
    for Aurora DSQL during the UX study.
    """

    def __init__(self):
        """
        Initialize Aurora DSQL connection

        TODO: Implement Aurora DSQL connection initialization
        - Read configuration from environment variables
        - Set up AWS authentication and Aurora DSQL client
        - Configure database connection parameters
        - Handle AWS credentials and region settings

        Environment variables to use:
        - AWS_REGION: AWS region for Aurora DSQL cluster
        - DSQL_CLUSTER_ENDPOINT: Aurora DSQL cluster endpoint
        - AWS_ACCESS_KEY_ID: AWS access key (or use IAM roles)
        - AWS_SECRET_ACCESS_KEY: AWS secret key (or use IAM roles)
        """
        super().__init__()
        self.provider_name = "Amazon Aurora DSQL"
        # TODO: Initialize Aurora DSQL connection
        self.connection = None

        # TODO: Read configuration from environment
        self.region = os.getenv("AWS_REGION")
        self.cluster_endpoint = os.getenv("DSQL_CLUSTER_ENDPOINT")
        self.db_user = os.getenv("DSQL_USER")
        self.db_port = int(os.getenv("DSQL_PORT", "5432"))
        self.db_name = os.getenv("DSQL_DB_NAME", "postgres")
        self.sslmode = os.getenv("SSLMODE", "require")
        self.schema = os.getenv("DSQL_SCHEMA", "tpcc")
        self.statement_timeout_ms = int(os.getenv("STATEMENT_TIMEOUT_MS", "0"))  # 0 = no timeout

        # TODO: Validate required configuration
        missing = []
        if not self.region:
            missing.append("AWS_REGION")
        if not self.cluster_endpoint:
            missing.append("DSQL_CLUSTER_ENDPOINT")
        if not self.db_user:
            missing.append("DSQL_USER")
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

        logger.info("Aurora DSQL init: region=%s, endpoint=%s, db=%s, user=%s, sslmode=%s, schema=%s",
                    self.region, self.cluster_endpoint, self.db_name, self.db_user, self.sslmode, self.schema)

        # TODO: Initialize Aurora DSQL client and connection
        # boto3 and psycopg2 are imported here to allow the file to load without these unless used
        self._boto3 = boto3
        self._psycopg2 = psycopg2
        self._psycopg2_extras = psycopg2.extras
        self._ensure_connection()

    def _generate_iam_token(self) -> str:
        """
        Generate a short-lived IAM DB auth token for Aurora DSQL (PostgreSQL-compatible endpoint).
        """
        dsql_client = self._boto3.client("dsql", region_name=self.region)

        # Admin role token vs. general connect token
        if str(self.db_user or "").lower() == "admin":
            token = dsql_client.generate_db_connect_admin_auth_token(
                Hostname=self.cluster_endpoint,
                Region=self.region,
                ExpiresIn=3600
            )
        else:
            token = dsql_client.generate_db_connect_auth_token(
                Hostname=self.cluster_endpoint,
                Region=self.region,
                ExpiresIn=3600
            )

        return token

    def _connect(self):
        boto3 = self._boto3
        psycopg2 = self._psycopg2
        psycopg2_extras = self._psycopg2_extras
        try:
            token = self._generate_iam_token()
            self.connection = psycopg2.connect(
                host=self.cluster_endpoint,
                port=self.db_port,
                user=self.db_user,
                password=token,  # IAM token
                dbname=self.db_name,
                sslmode=self.sslmode,
                connect_timeout=10,
                application_name="tpcc-webapp"
            )
            self.connection.autocommit = True

            # Per-session settings helpful for this app
            with self.connection.cursor() as cur:
                if self.statement_timeout_ms > 0:
                    cur.execute("SET statement_timeout = %s;", (self.statement_timeout_ms,))
                if self.schema:
                    cur.execute(f'SET search_path TO "{self.schema}";')

            logger.info("Aurora DSQL connection established successfully.")
        except Exception as e:
            logger.error(f"Failed to connect to Aurora DSQL: {e}")
            raise

    def _ensure_connection(self):
        """
        Open a connection if none exists or if previous connection was closed.
        """
        if self.connection is None:
            self._connect()
        else:
            try:
                with self.connection.cursor() as cur:
                    cur.execute("SELECT 1;")
            except Exception:
                # stale or closed: reconnect
                try:
                    self.connection.close()
                except Exception:
                    pass
                self.connection = None
                self._connect()

    
    def _approx_rowcount(self, table: str, schema: Optional[str] = None) -> int:
        """
        Fast approximate row count using catalog stats (O(1)).
        Prefers pg_stat_all_tables.n_live_tup, falls back to pg_class.reltuples.
        """
        self._ensure_connection()
        sc = schema or self.schema or 'public'
        # Try pg_stat_all_tables first
        try:
            rows = self.execute_query(
                "SELECT n_live_tup::bigint AS est FROM pg_stat_all_tables WHERE schemaname = %s AND relname = %s",
                (sc, table,),
            )
            if rows and rows[0].get("est") is not None:
                return int(rows[0]["est"]) or 0
        except Exception:
            pass
        # Fallback to pg_class.reltuples
        try:
            rows = self.execute_query(
                """
                SELECT c.reltuples::bigint AS est
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
                """,
                (sc, table,),
            )
            if rows and rows[0].get("est") is not None:
                return int(rows[0]["est"]) or 0
        except Exception:
            pass
        return 0

    def test_connection(self) -> bool:
        """
        Test connection to Aurora DSQL database

        TODO: Implement connection testing
        - Test connection to Aurora DSQL cluster
        - Execute a simple query to verify connectivity
        - Return True if successful, False otherwise
        - Log connection status for study data collection

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self._ensure_connection()
            # TODO: Implement connection test
            # Example: Execute "SELECT 1" query
            if not self.connection:
                logger.error("No Aurora DSQL connection available for testing.")
                return False
            with self.connection.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                if result and result[0] == 1:
                    logger.info("Aurora DSQL connection test succeeded.")
                    return True
                else:
                    logger.error("Aurora DSQL connection test failed: unexpected result.")
                    return False
        except Exception as e:
            logger.error(f"Aurora DSQL connection test failed: {str(e)}")
            return False

    def execute_query(
        self, query: str, params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute SQL query on Aurora DSQL

        TODO: Implement query execution
        - Handle parameterized queries safely
        - Convert Aurora DSQL results to standard format
        - Handle Aurora DSQL-specific data types
        - Implement proper error handling
        - Log query performance for study metrics

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            List of dictionaries representing query results
        """
        logger.warning("try to exec {}".format(query))
        psycopg2 = self._psycopg2
        psycopg2_extras = self._psycopg2_extras
        start_time = time.time()

        # Fast-path: approximate COUNT(*) without filters (e.g., "SELECT COUNT(*) AS count FROM customer")
        try:
            if params is None and isinstance(query, str):
                # Normalize whitespace and lowercase for matching
                norm = " ".join(query.strip().split())
                low = norm.lower()
                # Accept patterns like:
                #   select count(*) as count from schema.table
                #   select count(*) as cnt from table
                #   select count(*) from table
                #   ... optionally with "where 1=1"
                m = re.match(r"^select\s+count\s*\(\s*\*\s*\)\s*(?:as\s+(?P<alias>\w+))?\s+from\s+(?P<table>[\w\.\"']+)(?:\s+\w+)?(?:\s+where\s+1=1)?\s*;?\s*$", low)
                if m:
                    alias = m.group("alias") or "count"
                    table_expr = m.group("table")  # may be schema.table or quoted
                    # Extract bare table name (last identifier), strip quotes
                    tbl = table_expr.split('.')[-1].strip('"')
                    est = self._approx_rowcount(tbl)
                    elapsed = (time.time() - start_time) * 1000
                    logger.info(f"Fast approx COUNT for {tbl}: {est} (in {elapsed:.2f} ms)")
                    return [{alias: est}]
        except Exception:
            # If anything goes wrong, fall back to normal execution
            pass

        try:
            self._ensure_connection()
            with self.connection.cursor(cursor_factory=psycopg2_extras.RealDictCursor) as cur:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                # If SELECT, fetch results
                if cur.description:
                    results = cur.fetchall()
                    logger.info(f"Query executed. Rows: {len(results)}")
                    results = [dict(r) for r in results]
                else:
                    results = []
            elapsed = (time.time() - start_time) * 1000
            qlog = query.splitlines()[0][:200]
            logger.info(f"Aurora DSQL query executed in {elapsed:.2f} ms: {qlog}, results:{results}")
            return results
        except psycopg2.OperationalError as oe:
            # Handle expired token (reconnect and retry once)
            logger.warning(f"OperationalError on Aurora DSQL: {oe}. Attempting to reconnect and retry.")
            try:
                if self.connection:
                    self.connection.close()
            except Exception:
                pass
            self.connection = None
            self._connect()
            try:
                with self.connection.cursor(cursor_factory=psycopg2_extras.RealDictCursor) as cur:
                    if params:
                        cur.execute(query, params)
                    else:
                        cur.execute(query)
                    if cur.description:
                        results = cur.fetchall()
                        logger.info(f"Query executed after reconnect. Rows: {len(results)}")
                        results = [dict(r) for r in results]
                    else:
                        results = []
                elapsed = (time.time() - start_time) * 1000
                qlog = query.splitlines()[0][:200]
                logger.info(f"Aurora DSQL query re-executed in {elapsed:.2f} ms: {qlog}")
                return results
            except Exception as e2:
                logger.error(f"Aurora DSQL query execution failed after reconnect: {str(e2)}")
                raise
        except Exception as e:
            logger.error(f"Aurora DSQL query execution failed: {str(e)}")
            raise

    def get_provider_name(self) -> str:
        """Return the provider name"""
        return self.provider_name

    def get_orders(
        self,
        warehouse_id: Optional[int] = None,
        district_id: Optional[int] = None,
        customer_id: Optional[int] = None,
        status: Optional[str] = None,
        since: Optional[str] = None,
        limit: int = 50,
        page: int = 1,
        offset: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Return paginated orders with optional filters.
        Filters:
          - warehouse_id: o_w_id
          - district_id:  o_d_id
          - customer_id:  o_c_id
          - status: 'open' (in new_order), 'delivered' (o_carrier_id IS NOT NULL), or None
          - since:        ISO timestamp string; filters o_entry_d >= since
          - offset: when provided, takes precedence over page; zero-based row offset
        Returns a dict compatible with service usage:
          {
            "orders": List[dict],     # primary key
            "items":  List[dict],     # alias for compatibility
            "total":  int,
            "total_count": int,       # alias for compatibility
            "limit":  int,
            "page":   int,
            "offset": int,            # included when offset is used
            "has_next": bool,
            "has_prev": bool,
          }
        """
        self._ensure_connection()

        # Paging (support both page and explicit offset; offset wins if provided)
        limit = max(int(limit or 50), 1)
        if offset is not None:
            try:
                offset = max(int(offset), 0)
            except Exception:
                offset = 0
            page = max((offset // limit) + 1, 1)
        else:
            page = max(int(page or 1), 1)
            offset = (page - 1) * limit

        # WHERE clause
        where = ["1=1"]
        params_total: list = []
        params_page: list = []

        if warehouse_id is not None:
            where.append("o.o_w_id = %s")
            params_total.append(warehouse_id)
            params_page.append(warehouse_id)
        if district_id is not None:
            where.append("o.o_d_id = %s")
            params_total.append(district_id)
            params_page.append(district_id)
        if customer_id is not None:
            where.append("o.o_c_id = %s")
            params_total.append(customer_id)
            params_page.append(customer_id)
        # Optional status filter
        if status:
            st = str(status).strip().lower()
            if st in ("open", "new", "undelivered", "pending"):
                # Orders that still appear in NEW_ORDER (i.e., not fully processed)
                where.append("EXISTS (SELECT 1 FROM new_order no WHERE no.no_w_id = o.o_w_id AND no.no_d_id = o.o_d_id AND no.no_o_id = o.o_id)")
            elif st in ("delivered", "closed", "shipped"):
                # Treat delivered/closed as having a carrier id assigned
                where.append("o.o_carrier_id IS NOT NULL")
            elif st in ("all", "any"):
                pass  # no extra filter
            else:
                # Unknown status -> no filter (or could default to open)
                pass
        if since:
            where.append("o.o_entry_d >= %s")
            params_total.append(since)
            params_page.append(since)

        where_sql = " AND ".join(where)
        
        no_filters = (
            warehouse_id is None and district_id is None and customer_id is None and
            status is None and since is None
        )

        # Total count (use fast estimate if no filters to avoid full scan)
        if no_filters:
            total_count = self._approx_rowcount('orders')
        else:
            total_sql = (
                "SELECT COUNT(*) AS cnt FROM \"orders\" o "
                f"WHERE {where_sql}"
            )
            total_rows = self.execute_query(total_sql, tuple(params_total) if params_total else None)
            total_count = int(total_rows[0]["cnt"]) if total_rows else 0

        # Page query (orders only; fetch customer names in a second step to avoid huge sort+join)
        orders_sql = (
            "SELECT o.o_w_id AS w_id, o.o_d_id AS d_id, o.o_id, o.o_c_id AS c_id, "
            "       o.o_entry_d, o.o_carrier_id, o.o_ol_cnt, o.o_all_local "
            "FROM \"orders\" o "
            f"WHERE {where_sql} "
            "LIMIT %s OFFSET %s"
        )
        params_page_final = list(params_page) + [limit, offset]

        order_rows: List[Dict[str, Any]] = []
        try:
            order_rows = self.execute_query(orders_sql, tuple(params_page_final))
        except Exception as e:
            msg = str(e).lower()
            if "temp space" in msg or "temporary file" in msg or "out of memory" in msg:
                logger.warning(
                    "Orders page: falling back to no ORDER BY due to temp space limits: %s",
                    e,
                )
                orders_sql_no_order = (
                    "SELECT o.o_w_id AS w_id, o.o_d_id AS d_id, o.o_id, o.o_c_id AS c_id, "
                    "       o.o_entry_d, o.o_carrier_id, o.o_ol_cnt, o.o_all_local "
                    "FROM \"orders\" o "
                    f"WHERE {where_sql} "
                    "LIMIT %s OFFSET %s"
                )
                order_rows = self.execute_query(orders_sql_no_order, tuple(params_page_final))
            else:
                raise

        # Second step: batch fetch customer names for the current page
        if order_rows:
            # Build tuple list for IN ((w,d,c), ...). If tuple IN is not supported by DSQL, fallback to OR chain.
            triples = {(r["w_id"], r["d_id"], r["c_id"]) for r in order_rows}
            # Prefer tuple IN syntax
            try:
                placeholders = ", ".join(["(%s,%s,%s)"] * len(triples))
                flat_params: List[Any] = []
                for t in triples:
                    flat_params.extend(list(t))
                cust_sql = (
                    "SELECT c_w_id, c_d_id, c_id, c_first, c_middle, c_last "
                    "FROM customer WHERE (c_w_id, c_d_id, c_id) IN (" + placeholders + ")"
                )
                cust_rows = self.execute_query(cust_sql, tuple(flat_params))
            except Exception:
                # Fallback: OR chain
                or_parts = []
                flat_params = []
                for (w,d,cid) in triples:
                    or_parts.append("(c_w_id = %s AND c_d_id = %s AND c_id = %s)")
                    flat_params.extend([w,d,cid])
                cust_sql = (
                    "SELECT c_w_id, c_d_id, c_id, c_first, c_middle, c_last FROM customer WHERE "
                    + " OR ".join(or_parts)
                ) if or_parts else None
                cust_rows = self.execute_query(cust_sql, tuple(flat_params)) if cust_sql else []

            # Build a map for quick lookup
            cmap = {(r["c_w_id"], r["c_d_id"], r["c_id"]): r for r in cust_rows}
            for r in order_rows:
                info = cmap.get((r["w_id"], r["d_id"], r["c_id"]))
                if info:
                    r["c_first"] = info.get("c_first")
                    r["c_middle"] = info.get("c_middle")
                    r["c_last"] = info.get("c_last")
                else:
                    r.setdefault("c_first", None)
                    r.setdefault("c_middle", None)
                    r.setdefault("c_last", None)

        rows = order_rows

        has_prev = page > 1
        has_next = (page * limit) < total_count

        # Provide multiple key names for compatibility with different services
        result = {
            "orders": rows,
            "items": rows,
            "total": total_count,
            "total_count": total_count,
            "limit": limit,
            "page": page,
            "has_next": has_next,
            "has_prev": has_prev,
            "offset": offset,
        }
        return result
    def close_connection(self):
        """
        Close database connections

        TODO: Implement connection cleanup
        - Close Aurora DSQL client connections
        - Clean up any connection pools
        - Log connection closure for study metrics
        """
        try:
            # TODO: Implement connection cleanup
            # TODO: Close client connections
            # TODO: Log cleanup completion
            if self.connection:
                try:
                    self.connection.close()
                    logger.info("Aurora DSQL connection closed successfully.")
                except Exception:
                    pass
                self.connection = None
        except Exception as e:
            logger.error(f"Connection cleanup failed: {str(e)}")
