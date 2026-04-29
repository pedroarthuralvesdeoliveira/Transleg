from __future__ import annotations

from pathlib import Path

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait

from transleg.core.config import Settings
from transleg.domain.exceptions import ScrapingError


class BrowserSession:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.download_dir = self.settings.resolved_download_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.driver = self._build_driver(self.download_dir)

    def _build_driver(self, download_dir: Path) -> webdriver.Chrome:
        options = Options()
        options.add_experimental_option(
            "prefs",
            {
                "download.default_directory": str(download_dir),
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
            },
        )
        options.add_argument("--disable-extensions")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--blink-settings=imagesEnabled=false")
        options.add_argument("--window-size=1440,1200")

        if self.settings.browser_headless:
            options.add_argument("--headless=new")

        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(self.settings.page_load_timeout)
        driver.set_script_timeout(self.settings.page_load_timeout)
        if hasattr(driver.command_executor, "_client_config"):
            driver.command_executor._client_config.timeout = self.settings.page_load_timeout
        return driver

    def get(self, url: str) -> None:
        self.driver.get(url)

    def wait_for_element(
        self,
        by: str,
        value: str,
        timeout: int | None = None,
    ) -> WebElement:
        effective_timeout = timeout or self.settings.default_wait_timeout
        try:
            return WebDriverWait(self.driver, effective_timeout).until(
                ec.presence_of_element_located((by, value))
            )
        except TimeoutException as exc:
            raise ScrapingError(
                f"Elemento nao encontrado dentro do timeout: by={by}, value={value}"
            ) from exc

    def find_element(self, by: str, value: str) -> WebElement:
        return self.driver.find_element(by, value)

    def click_js(self, element: WebElement) -> None:
        self.driver.execute_script("arguments[0].click();", element)

    def set_value_js(self, element: WebElement, value: str) -> None:
        self.driver.execute_script("arguments[0].value = arguments[1];", element, value)
        self.driver.execute_script(
            "arguments[0].dispatchEvent(new Event('blur'));",
            element,
        )

    def clear_field(self, field_id: str) -> None:
        element = self.find_element(By.ID, field_id)
        element.clear()

    def click_field(self, field_id: str) -> None:
        self.find_element(By.ID, field_id).click()

    def select_value(self, field_id: str, value: str) -> None:
        Select(self.find_element(By.ID, field_id)).select_by_value(value)

    def quit(self) -> None:
        self.driver.quit()

    def __enter__(self) -> "BrowserSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.quit()

