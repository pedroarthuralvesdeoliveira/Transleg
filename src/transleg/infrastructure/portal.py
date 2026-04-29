from __future__ import annotations

import time
from datetime import date

from selenium.webdriver.common.by import By

from transleg.core.config import Settings
from transleg.domain.exceptions import ConfigurationError, ReportGenerationError
from transleg.domain.models import DateWindow, DownloadedReport, ReportSpec
from transleg.infrastructure.browser import BrowserSession
from transleg.infrastructure.downloads import DownloadWatcher


class AleffPortalClient:
    def __init__(self, browser: BrowserSession, settings: Settings) -> None:
        self.browser = browser
        self.settings = settings

    def login(self) -> None:
        if not self.settings.carrier_code:
            raise ConfigurationError("TRANSLEG_CARRIER_CODE nao foi configurada.")
        if not self.settings.portal_username:
            raise ConfigurationError("TRANSLEG_PORTAL_USERNAME nao foi configurada.")
        if not self.settings.portal_password:
            raise ConfigurationError("TRANSLEG_PORTAL_PASSWORD nao foi configurada.")

        self.browser.get(self.settings.portal_base_url)
        self.browser.wait_for_element(By.ID, "transportadora").send_keys(
            self.settings.carrier_code
        )
        self.browser.find_element(By.ID, "username").send_keys(
            self.settings.portal_username
        )
        self.browser.find_element(By.ID, "password").send_keys(
            self.settings.portal_password
        )
        self.browser.find_element(By.ID, "acesso").click()

    def open_report(self, spec: ReportSpec) -> None:
        self.browser.click_js(self.browser.wait_for_element(By.ID, spec.menu_module_id))
        self.browser.click_js(self.browser.wait_for_element(By.ID, spec.report_code_id))
        self.browser.click_js(
            self.browser.wait_for_element(
                By.XPATH,
                f"//a[contains(@href, '{spec.report_link_fragment}')]",
            )
        )

    def download_report(self, spec: ReportSpec, window: DateWindow) -> DownloadedReport:
        self.open_report(spec)
        self._fill_filters(spec, window.start, window.end)
        watcher = DownloadWatcher(self.settings.resolved_download_dir)
        self.browser.click_js(self.browser.wait_for_element(By.ID, "btnGerarExcel"))
        self.browser.click_js(
            self.browser.wait_for_element(
                By.XPATH,
                "//a[contains(@href, '/MonitorRelatorio/MonitorRelatorio')]",
                timeout=20,
            )
        )
        return self._monitor_until_ready(spec, watcher)

    def _fill_filters(self, spec: ReportSpec, start_date: date, end_date: date) -> None:
        self.browser.set_value_js(
            self.browser.wait_for_element(By.ID, "txtDataInicial"),
            start_date.strftime("%d/%m/%Y"),
        )
        self.browser.set_value_js(
            self.browser.find_element(By.ID, "txtDataFinal"),
            end_date.strftime("%d/%m/%Y"),
        )

        for field_id, value in spec.select_fields.items():
            self.browser.select_value(field_id, value)

        for field_id in spec.clear_fields:
            self.browser.clear_field(field_id)

        for field_id in spec.click_fields:
            self.browser.click_field(field_id)

        if spec.radio_button_id:
            radio = self.browser.wait_for_element(By.ID, spec.radio_button_id)
            self.browser.driver.execute_script(
                """
                arguments[0].checked = true;
                arguments[0].click();
                arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
                """,
                radio,
            )

    def _monitor_until_ready(
        self,
        spec: ReportSpec,
        watcher: DownloadWatcher,
        max_attempts: int = 30,
    ) -> DownloadedReport:
        attempts = 0

        while attempts < max_attempts:
            attempts += 1
            self.browser.click_js(
                self.browser.wait_for_element(
                    By.XPATH,
                    "//label[contains(@onclick, 'monitor.Atualizar')]",
                )
            )
            time.sleep(5)

            first_row = self.browser.wait_for_element(By.ID, "GridMonitorRelatorio_DXDataRow0")
            columns = first_row.find_elements(By.TAG_NAME, "td")
            if len(columns) < 9:
                continue

            description = columns[3].text.strip().upper()
            file_type = columns[6].text.strip().upper()
            status = columns[7].text.strip().upper()
            message = columns[8].text.strip().upper()

            if status != "CONCLUÍDO":
                continue

            if "NÃO FORAM ENCONTRADOS DADOS" in message:
                return DownloadedReport(
                    downloaded=False,
                    message="Nenhum dado encontrado para o periodo solicitado.",
                )

            if (
                description == spec.monitor_description.upper()
                and file_type == "EXCEL"
                and "BAIXAR" in message
            ):
                link = columns[8].find_element(By.TAG_NAME, "a")
                self.browser.click_js(link)
                file_path = watcher.wait_for_new_file(
                    prefix=spec.file_prefix,
                    timeout=self.settings.download_timeout,
                )
                if file_path is None:
                    raise ReportGenerationError(
                        "O portal liberou o download, mas o arquivo nao apareceu no diretorio."
                    )
                return DownloadedReport(
                    downloaded=True,
                    message="Download concluido com sucesso.",
                    file_path=file_path,
                )

            raise ReportGenerationError(
                f"Monitor finalizou com estado inesperado: descricao={description}, "
                f"status={status}, mensagem={message}"
            )

        raise ReportGenerationError(
            f"O relatorio nao ficou pronto apos {max_attempts} tentativas."
        )

