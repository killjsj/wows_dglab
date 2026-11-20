import inspect
import os
import zipfile
import time
import re
import asyncio
import threading
from datetime import datetime
import yaml
import apiLoader
import builtins
from apis.api import shockApiBase

WOWS_PATH = r"damage_log.txt"
targetLog = r"damage_log.txt"
WINDOW_SEC = 0.2
USINGAPI: list[shockApiBase] = []
ADD_STRENGTH_WHEN_HIT_IN_THIS_ROUND = 0.12
org_print = print
def TimestampedPrint(*args, **kwargs):
    # with print_lock:
        api_name = ""
        stack = inspect.stack()
        for frame_info in stack[1:5]:  # 跳过当前函数 限制4层
            frame = frame_info.frame
            if 'self' in frame.f_locals:
                obj = frame.f_locals['self']
                if hasattr(obj, 'ApiName'):
                    api_name = f" [API:{obj.ApiName}]"
                    break
        del stack  # 避免循环引用
        timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S") + f".{datetime.now().microsecond // 1000:03d}" + f"]{api_name}"
        org_print(timestamp, *args, **kwargs)

class DamageMonitor:
    def __init__(self, on_damaged_callback,on_died_callback=None,on_restart_callback=None):
        self.on_damaged = on_damaged_callback
        self.on_died = on_died_callback
        self.on_restart = on_restart_callback

        self.max_health = None
        self._stop_event = threading.Event()
        self._last_position = 0
        self._damage_queue = []
        self._flush_deadline = 0
        self._lock = threading.Lock()
        self._processing = False
        self._thread = threading.Thread(target=self._run, daemon=False)
        self._loop = None
        self._last_file_size = 0
        self._timer_active = False
        self._window_start = 0

    def start(self):
        self._async_thread = threading.Thread(target=self._run_async_loop, daemon=True)
        self._async_thread.start()
        self._thread.start()
        

    def stop(self):
        self._stop_event.set()
        self._thread.join(timeout=0.2)
        self._flush_queue(force=True)
        if self._loop and self._loop.is_running():
            self._loop.stop()
            # self._loop.call_soon_threadsafe(self._loop.stop)

    def _run_async_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _run(self):
        self._init_file_state()
        while not self._stop_event.is_set():
            has_new = self._check_new_lines()
            if has_new:
                self._flush_deadline = time.time() + WINDOW_SEC
            self._flush_queue()
            time.sleep(0.05)

    def _init_file_state(self):
        try:
            self._damage_queue.clear()
            self._timer_active = False
            if self.on_restart and self._loop and self._loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self.on_restart(), self._loop
                )
            with open(targetLog, "r",encoding='utf-8') as f:
                first_line = f.readline().strip()
                if first_line.startswith("MaxHealth,"):
                    self.max_health = float(first_line.split(",")[1])
                    print(f"[Init] Max Health: {self.max_health}")
                else:
                    self.max_health = None
                # 移动到文件末尾
                f.seek(0, 2)
                self._last_position = f.tell()
                self._last_file_size = self._last_position
                print(f"[Init] 初始位置: {self._last_position}")
        except (FileNotFoundError, OSError) as e:
            if isinstance(e, OSError) and e.errno != 2:
                print(f"[Init] 无法访问文件 {targetLog}: {e}")
            print(f"[Init] 文件 {targetLog} 不存在，等待创建...")
            self._last_position = 0
            self._last_file_size = 0
            self.max_health = None
        except Exception as e:
            print(f"[Init] 初始化文件失败: {e}")

    def _check_new_lines(self):
        try:
            if not os.path.exists(targetLog):
                return

            current_size = os.path.getsize(targetLog)

            # 文件被清空检测
            if current_size < self._last_file_size:
                print(
                    f"[检测] 文件被清空 (旧大小:{self._last_file_size}, 新大小:{current_size})"
                )
                self._init_file_state()
                return

            # 没有新数据
            if current_size <= self._last_position:
                return

            # 读取所有新数据
            with open(targetLog, "r",encoding='utf-8') as f:
                f.seek(self._last_position)
                content = f.read()  # 一次性读取所有新内容
                new_lines = content.strip().split("\n") if content.strip() else []

                if new_lines:
                    self._last_position = f.tell()
                    self._last_file_size = current_size
                    for line in new_lines:
                        line = line.strip()
                        if not line:
                            continue
                        if line.startswith("MaxHealth,"):
                            with self._lock:
                                self.max_health = float(line.split(",")[1])
                                print(f"[更新] Max Health: {self.max_health}")
                            continue
                        damages = self._parse_damages(line)
                        died = self._parse_died(line)
                        if died:
                            print("[died]true!")
                            if self.on_died and self._loop and self._loop.is_running():
                                asyncio.run_coroutine_threadsafe(
                                    self.on_died(self), self._loop
                                )
                        if damages:
                            with self._lock:
                                if len(self._damage_queue) == 0:
                                    
                                    # self._damage_queue.clear()
                                    total = sum(damages)
                                    # print(f"[触发] 首次伤害: {total:.2f}")
                                    self._timer_active = True

                                    if self.on_damaged and self._loop and self._loop.is_running():
                                        asyncio.run_coroutine_threadsafe(
                                            self.on_damaged(total, self), self._loop
                                        )
                                    self._window_start = time.time()
                                self._damage_queue.extend(damages)
        except Exception as e:
            print(f"[读取错误] {e}")
            import traceback

            traceback.print_exc()

    @staticmethod
    def _parse_damages(line):
        pattern = r"damage:\s*([0-9.]+)"
        matches = re.findall(pattern, line)
        if matches:
            return [float(m) for m in matches]
        return []
    @staticmethod
    def _parse_died(line) -> bool:
        pattern = r"isDied:(True|False)"
        matches = re.findall(pattern, line)
        if matches:
            for m in matches:
                if m == "True":
                    return True
                if m == "False":
                    return False
        return False
    def _flush_queue(self,force=False):
        now = time.time()

        with self._lock:
            if not self._timer_active and not force:
                return
            if now - self._window_start >= WINDOW_SEC:
                if len(self._damage_queue) > 0:
                    total = sum(self._damage_queue)
                    count = len(self._damage_queue)
                    if count > 1 and not force:
                        if self.on_damaged and self._loop and self._loop.is_running():
                            asyncio.run_coroutine_threadsafe(
                                self.on_damaged(total, self), self._loop
                            )
                    self._damage_queue.clear()

                else:
                    pass

                self._timer_active = False

class AsyncDamageHandler:
    """异步伤害处理器"""

    def __init__(self):
        pass
    
    async def on_died(self, monitor):
        for n in USINGAPI:
            try:
                await n.onDied()
            except Exception as e:
                print(f"[错误][{n.ApiName}] API onDied失败: {e}")

    async def on_restart(self):
        print("onRestart")
        for n in USINGAPI:
            try:
                await n.onRestart()
            except Exception as e:
                print(f"[错误][{n.ApiName}] API onRestart失败: {e}")
    async def on_damaged(self, damage_sum, monitor):
        if monitor.max_health and monitor.max_health > 0:
            for n in USINGAPI:
                try:
                    if ADD_STRENGTH_WHEN_HIT_IN_THIS_ROUND > 0:
                        n.addBaseStrength(ADD_STRENGTH_WHEN_HIT_IN_THIS_ROUND)
                    await n.onDamaged(damage_sum, monitor.max_health)
                except Exception as e:
                    print(f"[错误][{n.ApiName}] API失败: {e}")
        else:
            pass
            print(
                f"[{datetime.now().strftime('%H:%M:%S.%f')}] 聚合伤害: {damage_sum:.2f} (未知最大HP)"
            )

    async def close(self):
        for n in USINGAPI:
            try:
                if hasattr(n, "close"):
                    await n.close()
            except Exception as e:
                print(f"[错误][API:{n.ApiName}] API失败: {e}")


# ========== 主程序 ==========
if __name__ == "__main__":
    # 加载配置
    builtins.print = TimestampedPrint
    print("ai太好用了你们知道吗")
    config = None
    try:
        with open("config.yaml", "r",encoding='utf-8') as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
    except FileNotFoundError:
        print("[配置] config.yaml 不存在，使用默认配置")
    except Exception as e:
        print(f"[配置] 加载失败: {e}")
    USINGAPI_NAMES = []
    if config:
        WOWS_PATH = config.get("wows_path", WOWS_PATH)
        WINDOW_SEC = config.get("window_sec", WINDOW_SEC)
        # MUL_INT = config.get("shock", {}).get("mul_int", MUL_INT)
        ADD_STRENGTH_WHEN_HIT_IN_THIS_ROUND = config.get("shock", {}).get("addStrengthWhenHitInThisRound", ADD_STRENGTH_WHEN_HIT_IN_THIS_ROUND)
        USINGAPI_NAMES = config.get("shock", {}).get("usingApi", [])
        if not os.path.exists(WOWS_PATH):
            print(f"[配置] WOWS_PATH={WOWS_PATH} 不存在!")
            exit(1)
        VersionCode:str = "0"
        for n in os.listdir(WOWS_PATH + "bin\\"):
            if not n.isdigit():
                continue
            if int(n) > int(VersionCode):
                VersionCode = n
        print(f"[配置] 自动检测到版本号: {VersionCode}")
        targetLog = os.path.abspath(WOWS_PATH + f"bin\\{VersionCode}\\res_mods\\PnFMods\\ExportHPAndDied\\damage_log.txt")
        targetDir = os.path.abspath(WOWS_PATH + f"bin\\{VersionCode}\\res_mods\\PnFMods\\ExportHPAndDied\\")
        print(f"[配置] targetDir: {targetDir}")
        if not os.path.exists(targetDir):
            print(f"[配置] 插件 {targetDir} 不存在，正在创建...")
            zip_path = os.path.join(os.path.dirname(__file__), "ExportHPAndDied.zip")
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(os.path.dirname(targetDir))
                print(f"[配置] 插件解压完成 目标: {targetDir}")
            except Exception as e:
                print(f"[配置] 插件解压失败: {e}")
                exit(1)
            
        print(f"[配置] WOWS_PATH={WOWS_PATH}")
        print(f"[配置] targetLog={targetLog}")
        print(f"[配置] window_sec={WINDOW_SEC}")
        print(f"[配置] addStrengthWhenHitInThisRound={ADD_STRENGTH_WHEN_HIT_IN_THIS_ROUND}")
        if ADD_STRENGTH_WHEN_HIT_IN_THIS_ROUND > 2:
            print("[建议] addStrengthWhenHitInThisRound 设置过高 你每把都会挨很多发炮弹的 除非window_sec高否则我不建议太高")
        print(f"[配置] usingApi={USINGAPI_NAMES}")
        

    print("loading all apis...")
    apis = apiLoader.load_all_apis()

    print(f"[加载] 找到 {len(apis)} 个API:")
    for name, _ in apis.items():
        print(f"  - {name}")

    # 大小写不敏感匹配API
    matched_api_names:list[str] = []
    configured_api = USINGAPI_NAMES

    if configured_api:
        for capi in configured_api:
            for api_name in apis.keys():
                if api_name.lower() == capi.lower():
                    matched_api_names.append(api_name)
                    

    # 创建API实例
    if len(matched_api_names) <= 0:
        print(f"[错误] 配置的API '{configured_api}' 不存在，使用默认API")
        first_api_name = [next(iter(apis.keys()))]
        USINGAPI = apiLoader.create_api_instances(apis, first_api_name)
        if USINGAPI:
            print(f"[默认] 使用API: {first_api_name}")
    else:
        print(f"[使用] 使用API: {matched_api_names}")
        USINGAPI = apiLoader.create_api_instances(apis, matched_api_names)

    # 创建处理器
    handler = AsyncDamageHandler()

    # 创建回调函数
    async def async_on_damaged(damage_sum, monitor):
        await handler.on_damaged(damage_sum, monitor)
    async def async_on_died(monitor):
        await handler.on_died(monitor)
    async def async_on_restart():
        await handler.on_restart()
    monitor = DamageMonitor(async_on_damaged, async_on_died, async_on_restart)
    monitor.start()

    print("监控已启动，按 Ctrl+C 退出")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("正在停止...")
        monitor.stop()
        if monitor._loop and monitor._loop.is_running():
            asyncio.run_coroutine_threadsafe(handler.close(), monitor._loop)
        print("已停止")
