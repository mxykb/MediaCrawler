# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。
#
# 详细许可条款请参阅项目根目录下的LICENSE文件。
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。


import asyncio
import os
import random
import time
from asyncio import Task
from typing import Dict, List, Optional, Tuple

from playwright.async_api import BrowserContext, BrowserType, Page, Playwright, async_playwright
from tenacity import RetryError

import config
from base.base_crawler import AbstractCrawler
from config import CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES
from model.m_xiaohongshu import NoteUrlInfo
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from store import xhs as xhs_store
from tools import utils
from tools.cdp_browser import CDPBrowserManager
from var import crawler_type_var, source_keyword_var

from .client import XiaoHongShuClient
from .exception import DataFetchError
from .field import SearchSortType
from .help import parse_note_info_from_note_url, get_search_id
from .login import XiaoHongShuLogin


class XiaoHongShuCrawler(AbstractCrawler):
    # 类属性定义
    context_page: Page  # 浏览器页面对象
    xhs_client: XiaoHongShuClient  # 小红书API客户端
    browser_context: BrowserContext  # 浏览器上下文
    cdp_manager: Optional[CDPBrowserManager]  # CDP浏览器管理器

    def __init__(self) -> None:
        """
        初始化小红书爬虫对象，设置基础参数。
        """
        self.index_url = "https://www.xiaohongshu.com"  # 小红书首页URL
        self.user_agent = config.UA if config.UA else "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"  # 设置User-Agent，优先使用配置文件中的UA，否则用默认值
        self.cdp_manager = None  # CDP浏览器管理器，初始为None

    # ===================== start方法 =====================
    # 该方法是小红书爬虫的主入口，负责：
    #   1. 初始化代理池（如启用）
    #   2. 启动浏览器（支持CDP和标准模式）并注入反检测脚本
    #   3. 登录处理（如未登录自动登录）
    #   4. 创建API客户端，配置cookie/代理
    #   5. 根据配置选择并执行不同的爬取模式（关键词、指定笔记、创作者）
    #   6. 日志记录全流程关键节点
    # 设计思路：流程清晰，异常处理完善，便于扩展和维护。
    # ====================================================
    async def start(self) -> None:
        playwright_proxy_format, httpx_proxy_format = None, None  # 初始化代理格式变量（playwright用和httpx用）

        if config.ENABLE_IP_PROXY:  # 如果启用IP代理，则创建代理池
            ip_proxy_pool = await create_ip_pool(  # 创建IP代理池，池子大小和是否验证IP由配置决定
                config.IP_PROXY_POOL_COUNT, enable_validate_ip=True
            )
            ip_proxy_info: IpInfoModel = await ip_proxy_pool.get_proxy()  # 从代理池中获取一个可用的代理IP对象
            playwright_proxy_format, httpx_proxy_format = self.format_proxy_info(  # 将代理IP对象格式化为playwright和httpx需要的格式
                ip_proxy_info
            )

        async with async_playwright() as playwright:  # 使用异步上下文管理器启动Playwright
            if config.ENABLE_CDP_MODE:  # 判断是否启用CDP模式（即是否用本地Chrome调试协议）
                utils.logger.info("[XiaoHongShuCrawler] 使用CDP模式启动浏览器")  # 日志记录：使用CDP模式
                self.browser_context = await self.launch_browser_with_cdp(  # 启动CDP模式浏览器上下文
                    playwright, playwright_proxy_format, self.user_agent,
                    headless=config.CDP_HEADLESS
                )
            else:
                utils.logger.info("[XiaoHongShuCrawler] 使用标准模式启动浏览器")  # 日志记录：使用标准模式
                chromium = playwright.chromium  # 获取chromium对象
                self.browser_context = await self.launch_browser(  # 启动标准模式浏览器上下文
                    chromium, playwright_proxy_format, self.user_agent, headless=config.HEADLESS
                )
            await self.browser_context.add_init_script(path="libs/stealth.min.js")  # 向浏览器注入stealth.min.js脚本，防止被网站检测为自动化
            await self.browser_context.add_cookies(  # 添加webId cookie，减少滑块验证码出现概率
                [
                    {
                        "name": "webId",
                        "value": "xxx123",  # 任意值即可
                        "domain": ".xiaohongshu.com",
                        "path": "/",
                    }
                ]
            )
            self.context_page = await self.browser_context.new_page()  # 新建一个页面对象
            await self.context_page.goto(self.index_url)  # 打开小红书首页

            self.xhs_client = await self.create_xhs_client(httpx_proxy_format)  # 创建小红书API客户端，传入httpx代理格式
            if not await self.xhs_client.pong():  # 检查客户端是否已登录（通过pong方法）
                login_obj = XiaoHongShuLogin(  # 未登录则创建登录对象
                    login_type=config.LOGIN_TYPE,
                    login_phone="",  # 这里可填写手机号
                    browser_context=self.browser_context,
                    context_page=self.context_page,
                    cookie_str=config.COOKIES,
                )
                await login_obj.begin()  # 执行登录流程
                await self.xhs_client.update_cookies(  # 登录后更新客户端cookie
                    browser_context=self.browser_context
                )

            crawler_type_var.set(config.CRAWLER_TYPE)  # 设置全局变量，记录当前爬虫类型
            if config.CRAWLER_TYPE == "search":  # 根据配置选择不同的爬取模式
                await self.search()  # 关键词搜索模式
            elif config.CRAWLER_TYPE == "detail":
                await self.get_specified_notes()  # 指定笔记模式
            elif config.CRAWLER_TYPE == "creator":
                await self.get_creators_and_notes()  # 创作者主页模式
            else:
                pass  # 其他类型暂不处理

            utils.logger.info("[XiaoHongShuCrawler.start] Xhs Crawler finished ...")  # 日志记录：爬虫运行结束

    async def search(self) -> None:
        """
        搜索模式：根据关键词搜索笔记并获取评论信息。
        """
        utils.logger.info(  # 日志记录：开始关键词搜索
            "[XiaoHongShuCrawler.search] Begin search xiaohongshu keywords"
        )
        xhs_limit_count = 20  # 小红书搜索接口每页固定返回20条数据
        if config.CRAWLER_MAX_NOTES_COUNT < xhs_limit_count:  # 如果最大爬取数量小于每页数量，则自动调整为每页数量
            config.CRAWLER_MAX_NOTES_COUNT = xhs_limit_count
        start_page = config.START_PAGE  # 获取起始页配置
        for keyword in config.KEYWORDS.split(","):  # 遍历所有配置的关键词（用英文逗号分隔）
            source_keyword_var.set(keyword)  # 设置当前搜索关键词到全局变量
            utils.logger.info(  # 日志记录：当前搜索的关键词
                f"[XiaoHongShuCrawler.search] Current search keyword: {keyword}"
            )
            page = 1  # 初始化分页参数
            search_id = get_search_id()  # 获取搜索ID，用于标识本次搜索会话
            while (
                page - start_page + 1
            ) * xhs_limit_count <= config.CRAWLER_MAX_NOTES_COUNT:  # 分页爬取，直到达到最大爬取数量
                if page < start_page:  # 跳过起始页之前的页面
                    utils.logger.info(f"[XiaoHongShuCrawler.search] Skip page {page}")
                    page += 1
                    continue

                try:
                    utils.logger.info(  # 日志记录：当前搜索的关键词和页码
                        f"[XiaoHongShuCrawler.search] search xhs keyword: {keyword}, page: {page}"
                    )
                    note_ids: list[str] = []  # 初始化存储笔记ID的列表
                    xsec_tokens: list[str] = []  # 初始化存储token的列表
                    notes_res = await self.xhs_client.get_note_by_keyword(  # 调用搜索接口获取笔记列表
                        keyword=keyword,
                        search_id=search_id,
                        page=page,
                        sort=(
                            SearchSortType(config.SORT_TYPE)
                            if config.SORT_TYPE != ""
                            else SearchSortType.GENERAL
                        ),
                    )
                    utils.logger.info(  # 日志记录：搜索结果
                        f"[XiaoHongShuCrawler.search] Search notes res:{notes_res}"
                    )
                    if not notes_res or not notes_res.get("has_more", False):  # 检查是否还有更多内容
                        utils.logger.info("No more content!")
                        break
                    semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)  # 创建信号量控制并发数量
                    task_list = [  # 创建获取笔记详情的任务列表（过滤掉推荐和热门查询类型的item）
                        self.get_note_detail_async_task(
                            note_id=post_item.get("id"),
                            xsec_source=post_item.get("xsec_source"),
                            xsec_token=post_item.get("xsec_token"),
                            semaphore=semaphore,
                        )
                        for post_item in notes_res.get("items", {})
                        if post_item.get("model_type") not in ("rec_query", "hot_query")
                    ]
                    note_details = await asyncio.gather(*task_list)  # 并发执行所有任务，获取笔记详情
                    for note_detail in note_details:  # 处理获取到的笔记详情
                        if note_detail:
                            await xhs_store.update_xhs_note(note_detail)  # 保存笔记详情到数据库
                            await self.get_notice_media(note_detail)  # 获取笔记的媒体文件（图片、视频）
                            note_ids.append(note_detail.get("note_id"))  # 收集笔记ID
                            xsec_tokens.append(note_detail.get("xsec_token"))  # 收集token
                    page += 1  # 递增页码
                    utils.logger.info(  # 日志记录：本页获取到的笔记详情
                        f"[XiaoHongShuCrawler.search] Note details: {note_details}"
                    )
                    await self.batch_get_note_comments(note_ids, xsec_tokens)  # 批量获取笔记评论
                except DataFetchError:  # 数据获取失败，终止当前关键词的搜索
                    utils.logger.error(
                        "[XiaoHongShuCrawler.search] Get note detail error"
                    )
                    break

    async def get_creators_and_notes(self) -> None:
        """
        创作者模式：获取创作者的笔记并获取评论信息。
        """
        utils.logger.info(  # 日志记录：开始获取创作者信息
            "[XiaoHongShuCrawler.get_creators_and_notes] Begin get xiaohongshu creators"
        )
        for user_id in config.XHS_CREATOR_ID_LIST:  # 遍历配置的创作者ID列表
            createor_info: dict = await self.xhs_client.get_creator_info(  # 从网页HTML内容中获取创作者详细信息
                user_id=user_id
            )
            if createor_info:  # 如果成功获取到创作者信息，保存到数据库
                await xhs_store.save_creator(user_id, creator=createor_info)

            if config.ENABLE_IP_PROXY:  # 根据是否启用代理来设置不同的爬取间隔
                crawl_interval = random.random()  # 启用代理时使用较短的随机间隔
            else:
                crawl_interval = random.uniform(1, config.CRAWLER_MAX_SLEEP_SEC)  # 未启用代理时使用较长的随机间隔
            all_notes_list = await self.xhs_client.get_all_notes_by_creator(  # 获取创作者的所有笔记信息，回调处理每一批笔记详情
                user_id=user_id,
                crawl_interval=crawl_interval,
                callback=self.fetch_creator_notes_detail,  # 回调函数处理笔记详情
            )
            note_ids = []  # 提取笔记ID和token列表，用于后续获取评论
            xsec_tokens = []
            for note_item in all_notes_list:
                note_ids.append(note_item.get("note_id"))
                xsec_tokens.append(note_item.get("xsec_token"))
            await self.batch_get_note_comments(note_ids, xsec_tokens)  # 批量获取笔记评论

    async def fetch_creator_notes_detail(self, note_list: list[dict]):
        """
        并发获取指定笔记列表的详情并保存数据。
        Args:
            note_list: 笔记列表，每个元素包含note_id、xsec_source、xsec_token等信息
        """
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)  # 创建信号量控制并发数量
        task_list = [  # 为每个笔记创建获取详情的异步任务
            self.get_note_detail_async_task(
                note_id=post_item.get("note_id"),
                xsec_source=post_item.get("xsec_source"),
                xsec_token=post_item.get("xsec_token"),
                semaphore=semaphore,
            )
            for post_item in note_list
        ]
        note_details = await asyncio.gather(*task_list)  # 并发执行所有任务
        for note_detail in note_details:  # 处理获取到的笔记详情
            if note_detail:
                await xhs_store.update_xhs_note(note_detail)  # 保存笔记详情到数据库

    async def get_specified_notes(self):
        """
        指定笔记模式：获取指定笔记的信息和评论。
        """
        get_note_detail_task_list = []  # 创建获取笔记详情的任务列表
        for full_note_url in config.XHS_SPECIFIED_NOTE_URL_LIST:  # 遍历配置的笔记URL列表
            note_url_info: NoteUrlInfo = parse_note_info_from_note_url(full_note_url)  # 解析笔记URL，提取note_id、xsec_source、xsec_token等信息
            utils.logger.info(  # 日志记录：解析后的笔记URL信息
                f"[XiaoHongShuCrawler.get_specified_notes] Parse note url info: {note_url_info}"
            )
            crawler_task = self.get_note_detail_async_task(  # 创建获取笔记详情的异步任务
                note_id=note_url_info.note_id,
                xsec_source=note_url_info.xsec_source,
                xsec_token=note_url_info.xsec_token,
                semaphore=asyncio.Semaphore(config.MAX_CONCURRENCY_NUM),
            )
            get_note_detail_task_list.append(crawler_task)  # 添加到任务列表

        need_get_comment_note_ids = []  # 初始化需要获取评论的笔记ID列表
        xsec_tokens = []  # 初始化token列表
        note_details = await asyncio.gather(*get_note_detail_task_list)  # 并发执行所有获取笔记详情的任务
        for note_detail in note_details:  # 处理获取到的笔记详情
            if note_detail:
                need_get_comment_note_ids.append(note_detail.get("note_id", ""))  # 收集笔记ID
                xsec_tokens.append(note_detail.get("xsec_token", ""))  # 收集token
                await xhs_store.update_xhs_note(note_detail)  # 保存笔记详情到数据库
        await self.batch_get_note_comments(need_get_comment_note_ids, xsec_tokens)  # 批量获取笔记评论

    async def get_note_detail_async_task(
        self,
        note_id: str,
        xsec_source: str,
        xsec_token: str,
        semaphore: asyncio.Semaphore,
    ) -> Optional[Dict]:
        """
        异步获取笔记详情的任务。
        Args:
            note_id: 笔记ID
            xsec_source: 笔记来源参数
            xsec_token: 笔记访问令牌
            semaphore: 信号量，用于控制并发数量
        Returns:
            Optional[Dict]: 笔记详情字典，获取失败时返回None
        """
        note_detail_from_html, note_detail_from_api = None, None  # 初始化获取结果变量
        async with semaphore:  # 使用信号量控制并发数量
            if config.ENABLE_IP_PROXY:  # 根据代理配置设置不同的爬取间隔
                crawl_interval = random.random()  # 启用代理时使用较短的随机间隔
            else:
                crawl_interval = random.uniform(1, config.CRAWLER_MAX_SLEEP_SEC)  # 未启用代理时使用较长的随机间隔
            try:
                utils.logger.info(f"[get_note_detail_async_task] Begin get note detail, note_id: {note_id}")  # 日志记录：开始获取笔记详情
                note_detail_from_html: Optional[Dict] = (  # 第一步：尝试从HTML页面获取笔记详情（携带cookie）
                    await self.xhs_client.get_note_by_id_from_html(
                        note_id, xsec_source, xsec_token, enable_cookie=True
                    )
                )
                time.sleep(crawl_interval)  # 添加爬取间隔
                if not note_detail_from_html:  # 第二步：如果HTML方式失败，尝试不使用cookie重新获取
                    note_detail_from_html = (
                        await self.xhs_client.get_note_by_id_from_html(
                            note_id, xsec_source, xsec_token, enable_cookie=False
                        )
                    )
                    utils.logger.error(
                        f"[XiaoHongShuCrawler.get_note_detail_async_task] Get note detail error, note_id: {note_id}"
                    )
                if not note_detail_from_html:  # 第三步：如果HTML方式完全失败，尝试使用API获取
                    note_detail_from_api: Optional[Dict] = (
                        await self.xhs_client.get_note_by_id(
                            note_id, xsec_source, xsec_token
                        )
                    )
                note_detail = note_detail_from_html or note_detail_from_api  # 选择成功获取的结果
                if note_detail:  # 如果成功获取到笔记详情，添加必要的参数
                    note_detail.update(
                        {"xsec_token": xsec_token, "xsec_source": xsec_source}
                    )
                    return note_detail
            except DataFetchError as ex:  # 数据获取异常
                utils.logger.error(
                    f"[XiaoHongShuCrawler.get_note_detail_async_task] Get note detail error: {ex}"
                )
                return None
            except KeyError as ex:  # 关键字段缺失异常
                utils.logger.error(
                    f"[XiaoHongShuCrawler.get_note_detail_async_task] have not fund note detail note_id:{note_id}, err: {ex}"
                )
                return None

    async def batch_get_note_comments(
        self, note_list: List[str], xsec_tokens: List[str]
    ):
        """
        批量获取笔记评论。
        Args:
            note_list: 笔记ID列表
            xsec_tokens: 对应的xsec_token列表
        """
        if not config.ENABLE_GET_COMMENTS:  # 检查是否启用了评论爬取功能
            utils.logger.info(
                f"[XiaoHongShuCrawler.batch_get_note_comments] Crawling comment mode is not enabled"
            )
            return

        utils.logger.info(  # 日志记录：开始批量获取评论
            f"[XiaoHongShuCrawler.batch_get_note_comments] Begin batch get note comments, note list: {note_list}"
        )
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)  # 创建信号量控制并发数量
        task_list: List[Task] = []  # 初始化任务列表
        for index, note_id in enumerate(note_list):  # 为每个笔记创建获取评论的异步任务
            task = asyncio.create_task(
                self.get_comments(
                    note_id=note_id, 
                    xsec_token=xsec_tokens[index], 
                    semaphore=semaphore
                ),
                name=note_id,  # 使用笔记ID作为任务名称，便于调试
            )
            task_list.append(task)  # 添加到任务列表
        await asyncio.gather(*task_list)  # 并发执行所有获取评论的任务

    async def get_comments(
        self, note_id: str, xsec_token: str, semaphore: asyncio.Semaphore
    ):
        """
        获取单个笔记的评论信息（支持关键词过滤和数量限制）。
        Args:
            note_id: 笔记ID
            xsec_token: 笔记访问令牌
            semaphore: 信号量，用于控制并发数量
        """
        async with semaphore:  # 使用信号量控制并发数量
            utils.logger.info(
                f"[XiaoHongShuCrawler.get_comments] Begin get note id comments {note_id}"
            )
            if config.ENABLE_IP_PROXY:  # 根据代理配置设置不同的爬取间隔
                crawl_interval = random.random()  # 启用代理时使用较短的随机间隔
            else:
                crawl_interval = random.uniform(1, config.CRAWLER_MAX_SLEEP_SEC)  # 未启用代理时使用较长的随机间隔
            await self.xhs_client.get_note_all_comments(  # 获取笔记的所有评论
                note_id=note_id,
                xsec_token=xsec_token,
                crawl_interval=crawl_interval,
                callback=xhs_store.batch_update_xhs_note_comments,  # 回调函数批量保存评论
                max_count=CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES,  # 单个笔记最大评论获取数量
            )

    @staticmethod
    def format_proxy_info(
        ip_proxy_info: IpInfoModel,
    ) -> Tuple[Optional[Dict], Optional[Dict]]:
        """
        格式化代理信息，分别适用于playwright和httpx。
        Args:
            ip_proxy_info: 代理IP信息模型，包含IP、端口、用户名、密码等
        Returns:
            Tuple[Optional[Dict], Optional[Dict]]: 返回两个字典
                - 第一个：适用于playwright的代理格式
                - 第二个：适用于httpx的代理格式
        """
        playwright_proxy = {
            "server": f"{ip_proxy_info.protocol}{ip_proxy_info.ip}:{ip_proxy_info.port}",  # 拼接playwright代理server地址
            "username": ip_proxy_info.user,  # 代理用户名
            "password": ip_proxy_info.password,  # 代理密码
        }
        httpx_proxy = {
            f"{ip_proxy_info.protocol}": f"http://{ip_proxy_info.user}:{ip_proxy_info.password}@{ip_proxy_info.ip}:{ip_proxy_info.port}"  # 拼接httpx代理格式
        }
        return playwright_proxy, httpx_proxy  # 返回两个格式的代理配置

    async def create_xhs_client(self, httpx_proxy: Optional[str]) -> XiaoHongShuClient:
        """
        创建小红书客户端。
        Args:
            httpx_proxy: httpx格式的代理配置
        Returns:
            XiaoHongShuClient: 小红书API客户端实例
        """
        utils.logger.info(  # 日志记录：开始创建小红书API客户端
            "[XiaoHongShuCrawler.create_xhs_client] Begin create xiaohongshu API client ..."
        )
        cookie_str, cookie_dict = utils.convert_cookies(  # 从浏览器上下文中获取cookies并转换格式
            await self.browser_context.cookies()
        )
        xhs_client_obj = XiaoHongShuClient(  # 创建小红书客户端对象
            proxies=httpx_proxy,  # 代理配置
            headers={
                "User-Agent": self.user_agent,  # 用户代理
                "Cookie": cookie_str,  # Cookie字符串
                "Origin": "https://www.xiaohongshu.com",
                "Referer": "https://www.xiaohongshu.com",
                "Content-Type": "application/json;charset=UTF-8",
            },
            playwright_page=self.context_page,  # 传入playwright页面对象
            cookie_dict=cookie_dict,  # Cookie字典
        )
        return xhs_client_obj  # 返回客户端实例

    async def launch_browser(
        self,
        chromium: BrowserType,
        playwright_proxy: Optional[Dict],
        user_agent: Optional[str],
        headless: bool = True,
    ) -> BrowserContext:
        """
        启动浏览器并创建浏览器上下文（标准模式）。
        Args:
            chromium: Chromium浏览器类型
            playwright_proxy: playwright格式的代理配置
            user_agent: 用户代理字符串
            headless: 是否无头模式运行
        Returns:
            BrowserContext: 浏览器上下文对象
        """
        utils.logger.info(  # 日志记录：开始创建浏览器上下文
            "[XiaoHongShuCrawler.launch_browser] Begin create browser context ..."
        )
        if config.SAVE_LOGIN_STATE:  # 根据配置选择启动模式
            user_data_dir = os.path.join(  # 持久化模式：保存登录状态到本地目录
                os.getcwd(), 
                "browser_data", 
                config.USER_DATA_DIR % config.PLATFORM  # 根据平台创建不同的数据目录
            )
            browser_context = await chromium.launch_persistent_context(  # 启动持久化浏览器上下文
                user_data_dir=user_data_dir,
                accept_downloads=True,  # 允许下载
                headless=headless,
                proxy=playwright_proxy,
                viewport={"width": 1920, "height": 1080},  # 设置视口大小
                user_agent=user_agent,
            )
            return browser_context  # 返回浏览器上下文
        else:
            browser = await chromium.launch(  # 临时模式：每次启动全新的浏览器实例
                headless=headless, 
                proxy=playwright_proxy
            )
            browser_context = await browser.new_context(  # 创建新的浏览器上下文
                viewport={"width": 1920, "height": 1080},
                user_agent=user_agent
            )
            return browser_context  # 返回浏览器上下文

    async def launch_browser_with_cdp(self, playwright: Playwright, playwright_proxy: Optional[Dict],
                                     user_agent: Optional[str], headless: bool = True) -> BrowserContext:
        """
        使用CDP模式启动浏览器。
        Args:
            playwright: Playwright实例
            playwright_proxy: playwright格式的代理配置
            user_agent: 用户代理字符串
            headless: 是否无头模式运行
        Returns:
            BrowserContext: 浏览器上下文对象
        """
        try:
            self.cdp_manager = CDPBrowserManager()  # 创建CDP浏览器管理器
            browser_context = await self.cdp_manager.launch_and_connect(  # 启动并连接到Chrome浏览器
                playwright=playwright,
                playwright_proxy=playwright_proxy,
                user_agent=user_agent,
                headless=headless
            )
            browser_info = await self.cdp_manager.get_browser_info()  # 获取并显示浏览器信息，用于调试
            utils.logger.info(f"[XiaoHongShuCrawler] CDP浏览器信息: {browser_info}")
            return browser_context  # 返回浏览器上下文
        except Exception as e:  # CDP模式启动失败时，自动回退到标准模式
            utils.logger.error(f"[XiaoHongShuCrawler] CDP模式启动失败，回退到标准模式: {e}")
            chromium = playwright.chromium
            return await self.launch_browser(chromium, playwright_proxy, user_agent, headless)

    async def close(self):
        """
        关闭浏览器上下文和相关资源。
        """
        if self.cdp_manager:  # 如果使用CDP模式，需要特殊处理
            await self.cdp_manager.cleanup()  # 清理CDP管理器资源
            self.cdp_manager = None
        else:
            await self.browser_context.close()  # 标准模式直接关闭浏览器上下文
        utils.logger.info("[XiaoHongShuCrawler.close] Browser context closed ...")  # 日志记录：浏览器已关闭

    async def get_notice_media(self, note_detail: Dict):
        """
        获取笔记的媒体文件（图片和视频）。
        Args:
            note_detail: 笔记详情字典
        """
        if not config.ENABLE_GET_IMAGES:  # 检查是否启用了媒体文件获取功能
            utils.logger.info(
                f"[XiaoHongShuCrawler.get_notice_media] Crawling image mode is not enabled"
            )
            return
        await self.get_note_images(note_detail)  # 获取笔记的图片
        await self.get_notice_video(note_detail)  # 获取笔记的视频

    async def get_note_images(self, note_item: Dict):
        """
        获取笔记的图片文件。
        Args:
            note_item: 笔记信息字典
        """
        if not config.ENABLE_GET_IMAGES:  # 检查是否启用了图片获取功能
            return
        note_id = note_item.get("note_id")  # 提取笔记ID
        image_list: List[Dict] = note_item.get("image_list", [])  # 提取图片列表
        for img in image_list:  # 处理图片URL格式，使用默认URL
            if img.get("url_default") != "":
                img.update({"url": img.get("url_default")})
        if not image_list:  # 如果没有图片，直接返回
            return
        picNum = 0  # 初始化图片编号
        for pic in image_list:  # 逐一下载图片
            url = pic.get("url")
            if not url:
                continue
            content = await self.xhs_client.get_note_media(url)  # 下载图片内容
            if content is None:
                continue
            extension_file_name = f"{picNum}.jpg"  # 生成图片文件名
            picNum += 1
            await xhs_store.update_xhs_note_image(note_id, content, extension_file_name)  # 保存图片到存储系统

    async def get_notice_video(self, note_item: Dict):
        """
        获取笔记的视频文件。
        Args:
            note_item: 笔记信息字典
        """
        if not config.ENABLE_GET_IMAGES:  # 检查是否启用了媒体文件获取功能
            return
        note_id = note_item.get("note_id")  # 提取笔记ID
        videos = xhs_store.get_video_url_arr(note_item)  # 从笔记详情中提取视频URL列表
        if not videos:  # 如果没有视频，直接返回
            return
        videoNum = 0  # 初始化视频编号
        for url in videos:  # 逐一下载视频
            content = await self.xhs_client.get_note_media(url)  # 下载视频内容
            if content is None:
                continue
            extension_file_name = f"{videoNum}.mp4"  # 生成视频文件名
            videoNum += 1
            await xhs_store.update_xhs_note_image(note_id, content, extension_file_name)  # 保存视频到存储系统
