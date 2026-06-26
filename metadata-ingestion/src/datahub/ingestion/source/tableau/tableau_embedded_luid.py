"""Fetch embedded datasource LUIDs from Tableau Admin Insights (Site Content).

The Tableau Metadata API does not expose a `luid` field on EmbeddedDatasource.
The only programmatic source of this identifier is the Admin Insights
"Site Content" published datasource, queryable via VizQL Data Service (VDS).

This module provides a helper that:
1. Resolves the Site Content datasource LUID via the REST API
2. Queries VDS for all embedded datasource records
3. Returns a lookup map keyed by datasource name + parent project name

Requirements:
- The service account must have SiteAdministratorExplorer (or higher) role
- The Site Content datasource must have the "API Access" capability granted
"""

import logging
from typing import Dict, Optional, Tuple

import requests
from tableauserverclient import Server

logger = logging.getLogger(__name__)


def _resolve_site_content_luid(server: Server) -> Optional[str]:
    """Find the LUID of the Admin Insights 'Site Content' datasource."""
    try:
        import tableauserverclient as TSC

        logger.info(
            "Resolving Admin Insights 'Site Content' datasource LUID via REST API..."
        )
        req_options = TSC.RequestOptions()
        req_options.filter.add(
            TSC.Filter(
                TSC.RequestOptions.Field.Name,
                TSC.RequestOptions.Operator.Equals,
                "Site Content",
            )
        )
        datasources, _ = server.datasources.get(req_options)
        logger.info(
            f"REST API returned {len(datasources)} datasource(s) matching 'Site Content'"
        )
        for ds in datasources:
            if ds.name == "Site Content":
                logger.info(
                    f"Resolved Admin Insights Site Content datasource LUID: {ds.id}"
                )
                return ds.id
        logger.warning("Admin Insights 'Site Content' datasource not found on site")
        return None
    except Exception as e:
        logger.warning(f"Failed to resolve Site Content datasource LUID: {e}")
        return None


def _query_vds(
    server: Server,
    site_content_luid: str,
) -> Optional[list]:
    """Query VDS for all embedded datasource items from Site Content.

    Returns raw row data from the VDS response, or None on failure.
    """
    # VDS endpoint (Tableau Cloud):
    # POST https://{pod}.online.tableau.com/api/v1/vizql-data-service/query-datasource
    server_url = server._server_address  # e.g. https://dub01.online.tableau.com
    vds_url = f"{server_url}/api/v1/vizql-data-service/query-datasource"

    logger.info(
        f"Querying VDS for embedded datasource LUIDs: "
        f"url={vds_url}, datasourceLuid={site_content_luid}"
    )

    # Build query payload per VDS spec
    payload = {
        "datasource": {
            "datasourceLuid": site_content_luid,
        },
        "query": {
            "fields": [
                {"fieldCaption": "Item LUID"},
                {"fieldCaption": "Item Name"},
                {"fieldCaption": "Item Type"},
                {"fieldCaption": "Data Source Content Type"},
                {"fieldCaption": "Item Parent Project Name"},
            ],
            "filters": [
                {
                    "field": {"fieldCaption": "Item Type"},
                    "filterType": "SET",
                    "values": ["Datasource"],
                    "exclude": False,
                },
                {
                    "field": {"fieldCaption": "Data Source Content Type"},
                    "filterType": "SET",
                    "values": ["Embedded"],
                    "exclude": False,
                },
            ],
        },
        "options": {
            "returnFormat": "OBJECTS",
        },
    }

    # Use the server's authenticated session
    auth_token = server.auth_token
    headers = {
        "Content-Type": "application/json",
        "X-Tableau-Auth": auth_token,
    }

    try:
        resp = server._session.post(vds_url, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("data", [])
        logger.info(
            f"VDS query returned {len(rows)} embedded datasource records "
            f"from Site Content"
        )
        return rows
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else "unknown"
        body = ""
        if e.response is not None:
            try:
                body = e.response.text[:500]
            except Exception:
                pass
        logger.warning(
            f"VDS query for embedded datasource LUIDs failed "
            f"(HTTP {status}): {e}\n{body}"
        )
        return None
    except Exception as e:
        logger.warning(f"VDS query for embedded datasource LUIDs failed: {e}")
        return None


# The lookup key is (datasource_name, parent_project_name) to handle
# datasources with the same name in different projects.
EmbeddedLuidKey = Tuple[str, str]


def fetch_embedded_datasource_luids(
    server: Server,
) -> Dict[str, str]:
    """Fetch embedded datasource LUIDs from Admin Insights Site Content.

    Returns a dict mapping datasource_name -> luid.
    If multiple embedded datasources share the same name (across workbooks),
    we also build a secondary map by (name, project) for disambiguation.

    The primary return is a simple name -> luid map. For names that appear
    only once, this is unambiguous. For duplicates, the last one wins
    (callers can use the composite key map for precise matching).
    """
    site_content_luid = _resolve_site_content_luid(server)
    if not site_content_luid:
        return {}

    rows = _query_vds(server, site_content_luid)
    if not rows:
        return {}

    # Build lookup maps
    name_to_luid: Dict[str, str] = {}
    composite_to_luid: Dict[EmbeddedLuidKey, str] = {}
    name_count: Dict[str, int] = {}

    for row in rows:
        luid = row.get("Item LUID")
        name = row.get("Item Name")
        project = row.get("Item Parent Project Name") or ""

        if not luid or not name:
            continue

        name_to_luid[name] = luid
        composite_to_luid[(name, project)] = luid
        name_count[name] = name_count.get(name, 0) + 1

    duplicates = sum(1 for count in name_count.values() if count > 1)
    if duplicates:
        logger.info(
            f"Embedded datasource LUID map: {len(name_to_luid)} unique names, "
            f"{duplicates} names appear in multiple projects"
        )
    else:
        logger.info(
            f"Embedded datasource LUID map: {len(name_to_luid)} entries, "
            f"all names unique"
        )

    return name_to_luid
