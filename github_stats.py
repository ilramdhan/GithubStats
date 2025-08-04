#!/usr/bin/python3

import asyncio
import os
from typing import Dict, List, Optional, Set, Tuple, Any, cast

import aiohttp
import requests


class Queries:
    def __init__(self, username: str, access_token: str, session: aiohttp.ClientSession, max_connections: int = 10):
        self.username = username
        self.access_token = access_token
        self.session = session
        self.semaphore = asyncio.Semaphore(max_connections)

    async def query(self, generated_query: str) -> Dict:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        try:
            async with self.semaphore:
                resp = await self.session.post("https://api.github.com/graphql", headers=headers, json={"query": generated_query})
            return await resp.json()
        except:
            print("aiohttp failed for GraphQL query")
            resp = requests.post("https://api.github.com/graphql", headers=headers, json={"query": generated_query})
            return resp.json()

    async def query_rest(self, path: str, params: Optional[Dict] = None) -> Dict:
        headers = {"Authorization": f"token {self.access_token}"}
        path = path.lstrip("/")
        params = params or {}

        for _ in range(60):
            try:
                async with self.semaphore:
                    resp = await self.session.get(f"https://api.github.com/{path}", headers=headers, params=tuple(params.items()))
                if resp.status == 202:
                    print("202 Accepted, retrying...")
                    await asyncio.sleep(2)
                    continue
                return await resp.json()
            except:
                print("aiohttp failed for REST query")
                resp = requests.get(f"https://api.github.com/{path}", headers=headers, params=tuple(params.items()))
                if resp.status_code == 202:
                    print("202 Accepted, retrying...")
                    await asyncio.sleep(2)
                    continue
                return resp.json()

        print("Too many 202s. Data will be incomplete.")
        return {}

    @staticmethod
    def repos_overview(contrib_cursor: Optional[str] = None, owned_cursor: Optional[str] = None) -> str:
        return f"""{{
  viewer {{
    login
    name
    repositories(first: 100, isFork: false, after: {f'\"{owned_cursor}\"' if owned_cursor else 'null'}) {{
      pageInfo {{ hasNextPage endCursor }}
      nodes {{ nameWithOwner stargazers {{ totalCount }} forkCount languages(first: 10, orderBy: {{ field: SIZE, direction: DESC }}) {{ edges {{ size node {{ name color }} }} }} }}
    }}
    repositoriesContributedTo(first: 100, includeUserRepositories: false, after: {f'\"{contrib_cursor}\"' if contrib_cursor else 'null'}) {{
      pageInfo {{ hasNextPage endCursor }}
      nodes {{ nameWithOwner stargazers {{ totalCount }} forkCount languages(first: 10, orderBy: {{ field: SIZE, direction: DESC }}) {{ edges {{ size node {{ name color }} }} }} }}
    }}
  }}
}}"""

    @staticmethod
    def contrib_years() -> str:
        return """
query {
  viewer {
    contributionsCollection {
      contributionYears
    }
  }
}
"""

    @staticmethod
    def contribs_by_year(year: str) -> str:
        return f"""
    year{year}: contributionsCollection(from: \"{year}-01-01T00:00:00Z\", to: \"{int(year)+1}-01-01T00:00:00Z\") {{
      contributionCalendar {{ totalContributions }}
    }}
"""

    @classmethod
    def all_contribs(cls, years: List[str]) -> str:
        by_years = "\n".join(map(cls.contribs_by_year, years))
        return f"""
query {{
  viewer {{
    {by_years}
  }}
}}
"""

