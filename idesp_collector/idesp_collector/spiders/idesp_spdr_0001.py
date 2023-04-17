import json
import logging
from datetime import datetime

import scrapy


class IdespSpdr0001Spider(scrapy.Spider):
    name = "idesp_spdr_0001"
    allowed_domains = ["idesp.edunet.sp.gov.br"]
    start_urls = ["http://idesp.edunet.sp.gov.br/"]

    # Same urls required on task
    url_diretoria = "http://idesp.edunet.sp.gov.br/Home/Diretoria_Listar"
    url_municipio = "http://idesp.edunet.sp.gov.br/Home/Municipio_Listar"
    url_escola = "http://idesp.edunet.sp.gov.br/Home/Escola_Listar"

    # Url to send the final form.
    url_target_data = "http://idesp.edunet.sp.gov.br/Home/CarregaIdesp"

    # Error couny
    error_times = 0

    # Variable for dev control, to prevent DDOS flag for my IP (Reduced range for requests)
    dev = False

    def parse(self, response):
        """
        Get all years and regions on target form.
        :param response:
        :return:
        """
        year_list = response.xpath('//*[@id="cbAno"]/option/text()').getall()
        region_name_list = response.xpath('//*[@id="cbRegiao"]/option/text()').getall()
        limit = 1 if self.dev else len(year_list) + 1
        times = 0
        for y in year_list:
            if int(y) >= 2012 and times <= limit:
                for r_num, r_txt in enumerate(region_name_list):
                    if not str(r_txt).upper().startswith('SELECIONE'):
                        yield scrapy.FormRequest(
                            url=self.url_diretoria,
                            formdata={'ano': y, 'regiao': str(r_num), 'regiao_nome': r_txt},
                            callback=self.parse_diretoria
                        )
                times += 1

    def parse_diretoria(self, response):
        """
        Parallel works for diretoria, like jquery
        :param response:
        :return:
        """
        diretoria_list = self.json_converter(response.body, response=response)
        src_year_region = self.my_decoder(response.request.body)
        for direct in diretoria_list:
            src_year_region_direct = dict(
                diretoria_nome=direct.get('NM_DIRETORIA'),
                diretoria=direct.get('CD_DIRETORIADEENSINO'),
                **src_year_region.copy()
            )
            yield scrapy.FormRequest(
                url=self.url_municipio,
                formdata=src_year_region_direct,
                callback=self.parse_municipio
            )

    def parse_municipio(self, response):
        """
        Parallel works for Municipios, like jquery
        :param response:
        :return:
        """
        src_year_region_direct = self.my_decoder(response.request.body)
        municipio_list = self.json_converter(response.body, response=response)

        for city in municipio_list:
            src_year_region_direct_city = dict(
                municipio=city.get('ID_MUNICIPIO'),
                municipio_nome=city.get('NM_MUNICIPIO'),
                **src_year_region_direct.copy()
            )
            yield scrapy.FormRequest(
                url=self.url_escola,
                formdata=src_year_region_direct_city,
                callback=self.parse_escola
            )

    def parse_escola(self, response):
        src_year_region_direct_city = self.my_decoder(response.request.body)
        escola_list = self.json_converter(response.body, response=response)

        municipo_nome = src_year_region_direct_city['municipio_nome']
        ano = src_year_region_direct_city['ano']

        if self.dev:
            with open(f'./schools/escolas__{municipo_nome}_{ano}.txt', 'w') as file:
                yield file.write(response.body.decode('utf-8'))

        for school in escola_list:
            src_year_region_direct_city_school = dict(
                escola=school.get("ID_ESCOLA"),
                escola_nome=school.get('NM_ESCOLA'),
                ambiente="http://idesp.edunet.sp.gov.br/arquivos/",
                **src_year_region_direct_city.copy()
            )
            yield scrapy.FormRequest(
                url=self.url_target_data,
                formdata=src_year_region_direct_city_school,
                callback=self.parse_result
            )

    def parse_result(self, response):
        """
        Convert all collected data in the specific format
        :param response:
        :return:
        """
        src_year_region_direct_city_school = self.my_decoder(response.request.body)
        school_performance = self.json_converter(response.body, response=response)

        for school in school_performance:
            try:
                school['_REGICAO_NOME'] = src_year_region_direct_city_school['regiao_nome']
                school['_ANO'] = src_year_region_direct_city_school['ano']
                school['_DIRETORIA_ID'] = src_year_region_direct_city_school['diretoria']
                school['_MUNICIPIO_ID'] = src_year_region_direct_city_school['municipio']
                school['_ESCOLA_ID'] = src_year_region_direct_city_school['escola']
                school['_ERROR_ID'] = None
                school['_ERROR_INFOS'] = None
            except Exception as e:
                school['_REGICAO_NOME'] = None
                school['_ANO'] = None
                school['_DIRETORIA_ID'] = None
                school['_MUNICIPIO_ID'] = None
                school['_ESCOLA_ID'] = src_year_region_direct_city_school['escola']
                school['_ERROR_ID'] = src_year_region_direct_city_school['error_id']
                school['_ERROR_INFOS'] = src_year_region_direct_city_school['request_data']
                logging.error(e)

        return dict(school_performance=school_performance)

    @staticmethod
    def my_decoder(str_target, *args, **kwargs):
        """
        Simple decoder and pretier string formatter
        :param str_target:
        :param args:
        :param kwargs:
        :return:
        """
        decode = 'utf-8' if kwargs.get('decode') is None else kwargs.get('decode')
        separator = '&' if kwargs.get('separator') is None else kwargs.get('separator')

        src_str = str_target.decode(decode).split(separator)
        src_str = dict([(ss[0], ss[1]) for ss in [s.split('=') for s in src_str]])

        # Adjust for str of "São paulo e Região", +whitespaces
        src_str['regiao_nome'] = str(src_str['regiao_nome']) \
            .replace('+', ' ') \
            .replace('%C3%A3', 'ã') \
            .replace('%C3%83', 'Ã') \
            .upper()

        return src_str

    def json_converter(self, bin_str, response):
        """
        To handle litle errors.
        :param bin_str:
        :param response:
        :return:
        """
        try:
            ret_var = json.loads(bin_str)
        except Exception as e:
            self.error_times += 1
            if self.dev:
                with open(
                        f'./error_data_count_{self.error_times}__{datetime.now().strftime("%Y%m%d%H%M%S")}.txt',
                        'w') as file:
                    file.write(bin_str.decode())
            ret_var = [{'error_id': self.error_times, 'request_data': self.my_decoder(response.request.body)}]
            logging.error(e)
        return ret_var
