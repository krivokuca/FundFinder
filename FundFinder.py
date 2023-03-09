
"""
FundFinder
Version 1.0.0

Responsible for getting information pertaining to ETF's as well as end-of-day prices for those ETF's.
Currently only Vanguard is supported

TODO:
    - Add db abstraction & ORM
    - Add iShares, State Street, TD ETF support
    - Split into smaller files
    - Package project to pypi
"""

import datetime
import json
import os
import time
import urllib.parse
import uuid

import pytz
import requests

from routers.utils import typecheck_date


class FundFinder:

    def __init__(self, db_conn):
        """
        Parameters:
            - db_conn (SteadyDBConnection) :: A database connection
        """
        self.schema_file_path = os.path.dirname(
            os.path.realpath(__file__))+"/schemas"

        with open(self.schema_file_path, "r") as f:
            self.conf = json.load(f)

        self.conf = self.conf["FundFinderConfig"]
        self.fdb = FundDB(db_conn=db_conn)

    def lookup(self, fund_symbol=False, fund_name=False, commit=False):
        """
        Returns the metadata and constituent stock weight list for each fund provided.

        Parameters:
            - fund_symbol (bool|str|list[str]) :: If False, fund_name must be defined. If defined, fund_name must be false. This is a funds
                                                short, up-to 6 digit ticker that uniquely identifies it
            - fund_name (bool|str|list[str]) :: If False, fund_symbol must be defined. If defined, fund_name must be false. This is a funds
                                                long name or identifying query that could identify the fund.
            - commit (bool) :: If True, and if the fund doesnt already exist so, this flag will commit the fund to the database portfolio tables
        Returns:
            - fund_metadata(dict) :: A dictionary containing the following keys: fund_symbol, fund_data, weight_data
        """
        fund_symbol = fund_symbol.upper() if fund_symbol else False

        if not fund_symbol and not fund_name:
            raise Exception("Missing fund search values")

        if fund_symbol and fund_name:
            raise Exception(
                "Cannot search using both fund_name and fund_symbol")

        # check the db to make sure the fund isnt already there
        metadata = self.fdb.get_fund_metadata(fund_name, fund_symbol)
        if metadata:
            k = fund_symbol if fund_symbol else fund_name
            return Response(False, "Fund with identifier `{}` already exists".format(k))

        # TODO: add different fund provider resolution service here (blackrock, main street, etc. rn its just vanguard)

        fund_symbol, fund_data = self._vanguard_metadata_lookup(
            fund_symbol, fund_name)

        weight_data = self._extract_vanguard_weightlist(fund_symbol)
        if not fund_data and not weight_data:
            return {}

        if commit:
            has_metadata = self.fdb.get_fund_metadata(fund_symbol=fund_symbol)
            if not has_metadata:
                self.fdb.add_new_fund(fund_data, weight_data)

        return {
            "fund_symbol": fund_symbol,
            "fund_data": fund_data,
            "weight_data": weight_data
        }

    def index_vanguard(self):
        """
        Does a full crawl for every fund and its corresponding ticker weight list as well as the indexes
        listed in the schemas file

        Parameters:
            - commit (bool) :: If true, all the results will be committed to the database

        Returns:
            - True :: True, otherwise an exception
        """
        full_endpoint = self.conf["Schemas"][0]["ComprehensiveEndpoint"]
        req = requests.get(full_endpoint)
        if req.status_code != 200:
            raise Exception("Status code {} returned with {} body".format(
                req.status_code, req.text))

        funds = req.json()
        print("{} Funds found. Parsing...\n".format(
            len(funds["fund"]["entity"])))

        for f in funds["fund"]["entity"]:
            p = f["profile"]
            if not p or not "ticker" in p:
                continue

            # check if the fund already exists in the database
            if self.fdb.get_fund_metadata(fund_symbol=p["ticker"]):
                print("{} Already in DB".format(p["ticker"]))
                continue

            inception_datetime = datetime.datetime.strptime(
                p["inceptionDate"].split("T")[0], "%Y-%m-%d")

            flags = p["fundFact"]
            flags["is_etf"] = p["isETF"]
            flags["is_vlip"] = p["isVLIP"]
            flags["is_vvap"] = p["isVVAP"]
            flags["is_529"] = p["is529"]

            metadata = {"fundID": p["fundId"],
                        "ticker": p["ticker"],
                        "name": p["longName"],
                        "issuer": "Vanguard",
                        "issuer_id": p["fundId"],
                        "inception": inception_datetime,
                        "type": p["type"],
                        "expense_ratio": float(p["expenseRatio"]),
                        "risk": int(f["risk"]["code"]),
                        "category": p["category"],
                        "flags": flags}

            weight_list = self._extract_vanguard_weightlist(metadata["ticker"])
            weight_list = weight_list if weight_list else self._extract_institutional_vanguard_weightlist(
                p["fundId"])
            if not weight_list:
                print("Could not get weightlist for {}".format(
                    metadata["ticker"]))
                continue

            fund_pid = self.fdb.add_new_fund(metadata, weight_list)
            if not fund_pid:
                print("Could not add {} to the db".format(metadata["ticker"]))
                continue

            print("Added {} weights for fund {}".format(
                len(weight_list), metadata["ticker"]))

            # get prices
            price_history = self._vanguard_price_history(
                vanguard_id=metadata["issuer_id"])

            if not price_history:
                price_history = self._vanguard_institutional_price_history(
                    fundid=metadata["issuer_id"], start_date="2015-01-01", end_date=datetime.daGtetime.now().strftime("%Y-%m-%d"))

            if not price_history:
                print("No prices found for {} (Vanguard ID {})".format(
                    metadata["ticker"], metadata["issuer_id"]))
                continue

            price_errors = 0
            for price_obj in price_history:

                post_price = self.fdb.update_portfolio_price(
                    pid=fund_pid, created_on=price_obj["timestamp"], update_type=1, current_price=price_obj["price"], last_price=price_obj["price"], price_change=0.0, current_volume=-1, last_volume=-1, volume_change=0.0)

                if not post_price:
                    price_errors += 1

            if price_errors > 0:
                print("{} Exceptions occured during price indexing ({} prcnt)".format(
                    price_errors, (price_errors / len(price_obj)) * 100))

            # so we dont overload the API with requests and get throttled
            time.sleep(1)

        print("Finished indexing")

    def _vanguard_metadata_lookup(self, fund_symbol=False, fund_name=False):
        """
        Attempts to load metadata from Vanguard for a given symbol/fund name

        Returns:
            - fund_symbol (str) :: The ticker of the fund provided
            - extracted_profile (dict) :: The extracted vanguard metadata profile
        """
        if fund_name:  # search for the fund and try to use the first result
            request_url_schema = self.conf["Schemas"][0]["SearchEndpoint"].format(
                urllib.parse.quote_plus(fund_name))
            search_results = self._extract_vanguard_search(request_url_schema)
            if not search_results:
                raise Exception(
                    "Could not find any fund with {} keyword".format(fund_name))
            fund_symbol = search_results.values()[0]

        return fund_symbol, self._extract_vanguard_profile(fund_symbol)

    def _vanguard_current_price(self, pid=False, fund_symbol=False):
        """
        DEPRECATED: Does not work due to vanguard current price throttling, use vanguard_price_history
        Takes a pid or a fund_symbol and looks up the current price according to the vanguard price
        API
        Parameters:
            - pid (str) :: The pid
            - fund_symbol (str) :: The fund symbol

        Returns:
            - price_obj|false (dict)
        """

        if not pid or not fund_symbol:
            return {}

        if pid:
            resolved = self.fdb.resolve_pid(pid)
            if not resolved:
                return False

            if resolved["issuer"] != "Vanguard":
                return False

            fund_symbol = resolved["symbol"]

        endpoint = self.conf["Schemas"][0]["BaseEndpoint"].format(fund_symbol)
        endpoint = endpoint + self.conf["Schemas"][0]["URLMap"]["MarketPrice"]
        results = requests.get(endpoint)
        if results.status_code != 200:
            print("error 200")
            return False

        results = results.json()
        if results["errors"]:
            print("errors found")
            return False

        return {
            "date": datetime.datetime.fromisoformat(results["quotes"][0]["tradingDates"]["lastTradeDateTime"]),
            "adjustedClosePrice": float(results["quotes"][0]["pricing"]["adjustedClosePrice"]),
            "previousClosePrice": float(results["quotes"][0]["pricing"]["previousClosePrice"]),
            "volume": int(results["quotes"][0]["pricing"]["volume"]["todaysVolume"])
        }

    def _vanguard_price_history(self, pid=False, vanguard_id=False, date_range="1M"):
        """
        Gets the price history of a product from the API given the pid or the vanguard issuer id

        Parameters:
            - pid (str) :: The pid
            - vanguard_id (str) :: The vanguard issure id
            - date_range (str) :: Either 1M, 5M, or 1Y (stick to 1M)

        Returns:
            - price_history (list) :: A list of dicts with the keys "timestamp", "price" or False is throttled
        """
        if pid and vanguard_id:
            return False

        if pid:
            resolve_dict = self.fdb.resolve_pid(pid)
            if not resolve_dict:
                return False
            vanguard_id = resolve_dict["issuer_id"]

        endpoint = self.conf["Schemas"][0]["BaseEndpoint"].format(vanguard_id)
        endpoint = endpoint + \
            self.conf["Schemas"][0]["URLMap"]["PriceHistory"].format(
                date_range)

        response = requests.get(endpoint)

        if response.status_code != 200:
            print("FORBIDDEN for {}".format(vanguard_id))
            return False

        response = response.json()
        # get all the string timestamps into datetime objects
        if "marketPrice" not in response:
            return []
        if not isinstance(response["marketPrice"], list):
            return []
        if not response["marketPrice"][0]:
            print("No market prices found {}".format(vanguard_id))
            return []

        price_history = []
        for price in response["marketPrice"][0]["item"]:
            if "price" not in price:
                continue
            price_history.append({
                "timestamp": datetime.datetime.fromisoformat(price["asOfDate"]),
                "price": float(price["price"])
            })

        return price_history

    def _vanguard_institutional_price_history(self, fundid=False, start_date=False, end_date=False):
        """
        Looks up the price history for a portfolio given the portfolios fundid.

        Parameters:
            - fundid (str) :: The portfolios fund id
            - start_date (str) :: A string in the form of %Y-%m-%d, if false, will default to today - 1
            - end_date (str) :: A string in the form of %Y-%m-%d, if false, will default to today

        Returns:
            - price_history (list) :: A list of dicts with each dict having the keys "timestamp" and "price" (or False if throttled)
        """
        if not fundid:
            return False
        if start_date:
            start_date = typecheck_date(start_date, "%Y-%m-%d")
            if not start_date:
                raise ValueError("start_date must in the format %Y-%m-%d")
        if end_date:
            end_date = typecheck_date(end_date, "%Y-%m-%d")
            if not end_date:
                raise ValueError("end_date must be in the format %Y-%m-%d")

        if not start_date:
            start_date = datetime.datetime.now() - datetime.timedelta(days=1)
            start_date = start_date.replace(
                hour=0, minute=0, second=0, microsecond=0)
        if not end_date:
            end_date = start_date + datetime.timedelta(days=1)

        formatted_start = start_date.strftime("%Y-%m-%d")
        formatted_end = end_date.strftime("%Y-%m-%d")
        endpoint = self.conf["Schemas"][0]["InstitutionalEndpoint"].format(
            fundid, formatted_start, formatted_end)

        results = requests.get(endpoint)

        if results.status_code != 200:
            return []

        resp = results.json()
        prices = []

        for item in resp["priceHistoryItems"]:
            prices.append({
                "timestamp": datetime.datetime.strptime(item["effectiveDate"], "%Y-%m-%d"),
                "price": item["price"]
            })
        return prices

    def _extract_vanguard_search(self, search_url):
        """
        Takes a Vanguard search URL and extracts it as a formatted Dict
        """
        req = requests.get(search_url)
        if req.status_code != 200:
            raise Exception("{} code returned with body {}".format(
                req.status_code, req.text))
        search_response = json.loads(req.text[17:-1])
        if not search_response["results"]:
            return {}
        output_dict = {}
        for result in search_response["results"]:
            output_dict[result["name"]] = result["tickerSymbol"]
        return output_dict

    def _extract_vanguard_profile(self, fund_symbol):
        """
        Extracts a funds profile from vanguard given the funds symbol
        """
        request_url_schema = self.conf["Schemas"][0]["BaseEndpoint"].format(
            fund_symbol.upper()) + self.conf["Schemas"][0]["URLMap"]["Overview"]
        req = requests.get(request_url_schema)
        if req.status_code != 200:
            raise Exception("{} code returned with body".format(
                req.status_code, req.text))
        profile = req.json()
        if "name" in profile:
            return {}

        p = profile["fundProfile"]

        inception_datetime = datetime.datetime.strptime(
            p["inceptionDate"].split("T")[0], "%Y-%m-%d")

        flags = p["fundFact"]
        flags["is_etf"] = p["isETF"]
        flags["is_vlip"] = p["isVLIP"]
        flags["is_vvap"] = p["isVVAP"]
        flags["is_529"] = p["is529"]

        return {
            "fundID": p["fundId"],
            "ticker": p["ticker"],
            "name": p["longName"],
            "issuer": "Vanguard",
            "issuer_id": str(p["fundId"]),
            "inception": inception_datetime,
            "type": p["type"],
            "expense_ratio": float(p["expenseRatio"]),
            "risk": 1,  # TODO: HERE
            "category": p["category"],
            "flags": flags
        }

    def _extract_vanguard_weightlist(self, fund_symbol, offset=0):
        """
        Returns a list of a vanguard funds constiuent tickers. Each list contains a sublist
        with the following keys: [stock ticker, # of shares held, current market value of shares held, percent weight]

        Parameters:
            - fund_symbol (str) :: The ticker/symbol of the mutual fund
            - offset (int) :: The stock offset number to start from

        Returns:
            - weights (list) :: A list of lists with each sublist being the stock ticker, total number of shares and percentage of the
                                in that order
        """
        offset = 0
        has_next = True
        weights = []
        while has_next:
            weight_endpoint = self.conf["Schemas"][0]["BaseEndpoint"].format(
                fund_symbol) + self.conf["Schemas"][0]["URLMap"]["StockList"].format(offset)
            raw_weights = self._get_weights(weight_endpoint)
            if "next" not in raw_weights:
                has_next = False
            else:
                offset = offset + 501 if offset == 0 else offset + 500

            if not raw_weights["fund"]:
                return []

            for ticker in raw_weights["fund"]["entity"]:
                if not ticker["ticker"]:
                    continue

                weights.append([ticker["ticker"], int(ticker["sharesHeld"]), float(
                    ticker["marketValue"]), float(ticker["percentWeight"])])

        return weights

    def _extract_institutional_vanguard_weightlist(self, fundid, as_of="2022-06-30"):
        """
        Extracts the stock weightlist from the institutional endpoint

        Returns:
            - weights (list) :: a list of list with each sublist being the stock ticker,
                                total number of shares held, market value of those shares,
                                and the percentage in that order
        """
        endpoint = self.conf["Schemas"][0]["InstitutionalEndpoint"] + \
            self.conf["Schemas"][0]["URLMap"]["InstitutionalStockList"].format(
                fundid)
        results = requests.get(endpoint)

        if results.status_code != 200:
            return False
        body = results.json()
        weights = []
        for item in body[0]["holdingDetailItem"]:
            if "bticker" not in item.keys():
                continue
            if "marketVal" not in item.keys():
                continue

            weights.append(
                [item["bticker"], item["quantity"], item["marketVal"], item["mktValPercent"]])
        return weights

    def _get_weights(self, url):
        """
        Sends a request to the Vanguard weight API, returns a weight dictionary

        Returns:
            - weights (dict) :: The weights JSON object

        """
        req = requests.get(url)
        if req.status_code != 200:
            raise Exception("{} status code returned with body {}".format(
                req.status_code, req.text))
        weights = req.json()
        return weights

    def _get_pids(self):
        """
        Returns each pid being followed in the portfolios table
        """
        return self.fdb.get_pid_list()

    def _typecheck_date(date_string, date_format):
        """
        Checks to see if a date is properly formatted

        Returns:
            - false|datetime.datetime
        """
        try:
            parsed = datetime.datetime.strptime(date_string, date_format)
            return parsed
        except:
            return False


class PortfolioPriceEngine:
    """
    This is going to be deprecated soon by the portofino rust directory because it is very buggy and very slow.
    """

    def __init__(self, db_conn):
        self.schema_file_path = os.path.dirname(
            os.path.realpath(__file__))+"/schemas"

        with open(self.schema_file_path, "r") as f:
            self.conf = json.load(f)

        self.conf = self.conf["FundFinderConfig"]
        self.fdb = FundDB(db_conn)

    def get_nav_for_index(self, index_code="sp500", start_date=False, end_date=False):
        """
        Given an index_code, this function calculates the NAV (net asset value) for a given index. If the start_date
        and the end_date are False than it defaults to start_date= today - 40 trading days, end_date=today

        Parameters:
            - index_code (str) :: The index to calculate the NAV for (located in the schemas file)
            - start_date (str|datetime) :: Either a string in the form "%Y-%m-%d %H:%M:%S" or a datetime
            - end_date (str|datetime) :: Either a string or a datetime (same format as start_date :p )

        Returns:
            - nav_object (list|False) :: A list of nav dicts. Each dict contains the following keys:
                current_price, last_price, price_change, current_vol, last_vol, vol_change, current_volume, last_volume, volume_change,
                delta_1d, delta_5d, delta_10d, delta_20d, delta_120d, delta_240d

        """

        if index_code not in self.conf["IndexCodes"]:
            return False

        if isinstance(start_date, str):
            start_date = typecheck_date(start_date, "%Y-%m-%d")
            start_date = start_date if start_date else typecheck_date(
                start_date, "%Y-%m-%d %H:%M:%S")
            if not start_date:
                return False

        if isinstance(end_date, str):
            end_date = typecheck_date(end_date, "%Y-%m-%d")
            end_date = end_date if end_date else typecheck_date(
                end_date, "%Y-%m-%d %H:%M:%S")
            if not end_date:
                return False

        if not start_date:
            start_date = datetime.datetime.now().replace(
                hour=0, minute=0, second=0, microsecond=0)

        if not end_date:
            end_date = start_date - datetime.timedelta(days=40)

        weights = self.fdb.get_shares_held(index_code)
        if not weights:
            return False
        tickers = list(weights.keys())

        stock_prices = self.fdb.get_stock_prices(
            tickers, start_date, end_date, granular=False)

        if isinstance(stock_prices, bool) and not stock_prices:
            return False

        if isinstance(stock_prices, list) and len(stock_prices) == 0:
            print("NO PRICES FOUND FOR {}".format(index_code))
            return False

        # length of the list is the total number of days between start_date and end_date
        net_asset_values = []
        net_volume_values = []
        sorted_dates = []
        for x in stock_prices:
            if x["date"] not in sorted_dates:
                sorted_dates.append(x["date"])
        sorted_dates.sort()
        current_date = sorted_dates[0]
        interval = (sorted_dates[-1] - current_date).days
        print("interval: {} n {}".format(interval, len(sorted_dates)))
        for date in sorted_dates:
            prices = [x for x in stock_prices if x["date"] == date]
            nav = 0.0
            volume = 0.0
            for price in prices:
                nav = nav + price["close"] * weights[price["ticker"]]
                volume = volume + price["volume"] * weights[price["ticker"]]
            net_asset_values.append(nav)
            net_volume_values.append(volume)

        price_info = {
            "current_price": net_asset_values[-1],
            "last_price": net_asset_values[0],
            "price_change": (net_asset_values[-1] - net_asset_values[0]) / net_asset_values[0],
        }
        std_vol, norm_vol = self.stock_engine.volatility(net_asset_values)

        vol_info = {
            "current_vol": norm_vol,
            "last_vol": 0.0,
            "vol_change": 0.0
        }

        if net_volume_values[1] == 0:
            net_volume_change = 0.0
        else:
            net_volume_change = (
                net_volume_values[0] - net_volume_values[1]) / net_volume_values[1]

        volume_info = {
            "current_volume": net_volume_values[0],
            "last_volume": net_volume_values[1],
            "volume_change": net_volume_change,
        }
        try:
            delta_info = {
                "delta_1d": net_asset_values[0] - net_asset_values[1] / net_asset_values[1],
                "delta_5d": net_asset_values[0] - net_asset_values[5] / net_asset_values[5],
                "delta_10d": net_asset_values[0] - net_asset_values[10] / net_asset_values[10],
                "delta_20d": net_asset_values[0] - net_asset_values[19] / net_asset_values[19],
                "delta_120d": 0.0,
                "delta_240d": 0.0
            }
        except:
            delta_info = {
                "delta_1d": 0.0,
                "delta_5d": 0.0,
                "delta_10d": 0.0,
                "delta_20d": 0.0,
                "delta_120d": 0.0,
                "delta_240d": 0.0
            }

        nav_object = price_info | vol_info | volume_info | delta_info
        return nav_object

    def get_nav_for_portfolio(self, pids):
        """
        Gets the net asset value of the portfolio given the stocks and the shares held in the portfolio.
        Uses the most update-to-date volume

        Parameters:
            - pids (list) :: The list of pids to update the price for
        Returns:

        """
        for pid in pids:

            price_info = {
                "current_price": 0.0,
                "last_price": 0.0,
                "price_change": 0.0,
            }

            vol_info = {
                "current_vol": 0.0,
                "last_vol": 0.0,
                "vol_change": 0.0
            }
            volume_info = {
                "current_volume": 0.0,
                "last_volume": 0.0,
                "volume_change": 0.0,
            }
            delta_info = {
                "delta_1d": 0.0,
                "delta_5d": 0.0,
                "delta_10d": 0.0,
                "delta_20d": 0.0,
                "delta_120d": 0.0,
                "delta_240d": 0.0
            }
            metadata = self.fdb.get_fund_metadata(pid=pid)
            shares = self.fdb.get_shares_held(pid)
            if not metadata or not shares:
                continue

            # TODO rewrite this to check the portfolio_indicators table first because this is horribly
            # inefficient to leave it this way.

            past_closures = {
                0: self.fdb.most_recent_close(list(shares.keys())),
                1: self.fdb.most_recent_close(list(shares.keys()), day_delta=1),
                5: self.fdb.most_recent_close(list(shares.keys()), day_delta=5),
                10: self.fdb.most_recent_close(list(shares.keys()), day_delta=10),
                20: self.fdb.most_recent_close(list(shares.keys()), day_delta=20),
                120: self.fdb.most_recent_close(list(shares.keys()), day_delta=120),
                240: self.fdb.most_recent_close(list(shares.keys()), day_delta=240)
            }

            for close_date in past_closures:
                close_list = past_closures[close_date]
                if close_date == 0:
                    share_totals = sum([x["close"]*shares[x["ticker"]]
                                        for x in close_list])

    def typecheck_date(date_string, date_format):
        """
        Checks to see if a date is properly formatted

        Returns:
            - false|datetime.datetime
        """
        try:
            parsed = datetime.datetime.strptime(date_string, date_format)
            return parsed
        except:
            return False


class Response:
    def __init__(self, status=True, message=False):
        self.status = status
        self.message = message


class FundDB:
    def __init__(self, db_conn):
        self.db = db_conn

    def get_fund_metadata(self, fund_name=False, fund_symbol=False, pid=False):
        """
        Attempts to grab all the metadata from a fund given the fund symbol, fund name or the PID of the portfolio

        Returns:
            - metadata :: A dictionary of metadata
        """

        if fund_name and fund_symbol and pid:
            raise Exception(
                "Funds can only be returned by fund_name, fund_symbol or pid, but not all 3.")

        cursor = self.db.cursor()
        selection_query = "SELECT `pid`, `issuer`, `name`, `symbol`, `structure`, `expense_ratio`, `risk`, `inception` FROM `portfolios` "

        if fund_name:
            selection_query += "WHERE `name` = %s LIMIT 1;"
            cursor.execute(selection_query, fund_name)

        elif fund_symbol:
            selection_query += "WHERE `symbol` = %s LIMIT 1;"
            cursor.execute(selection_query, fund_symbol)

        elif pid:
            selection_query += "WHERE `pid = %s LIMIT 1;"
            cursor.execute(selection_query, pid)

        results = cursor.fetchall()

        return results

    def get_shares_held(self, pid):
        """
        Gets a dict of shares held of each stock in a portfolio given the portfolios pid

        Returns:
            - formatted_dict (dict) :: A dict in the format {ticker: shares_held (float)}
        """

        cursor = self.db.cursor()

        sql = "SELECT `ticker`, `shares_held`, `percent_weight` FROM `portfolio_weights` WHERE `pid` = %s;"
        cursor.execute(sql, pid)
        results = cursor.fetchall()
        formatted_dict = {}
        for result in results:
            if result["shares_held"]:
                formatted_dict[result["ticker"]] = result["shares_held"]
            else:
                formatted_dict[result["ticker"]
                               ] = result["percent_weight"] / 100
        return formatted_dict

    def add_new_fund(self, fund_metadata, fund_weights):
        """
        Adds a new fund to the portfolios, portfolio_flags and portfolio_indicators

        Parameters:
            - fund_metadata (dict) :: The fund metadata dictionary
            - fund_weights (list) :: The fund price list

        Returns:
            - pid (str|False) :: return the pid if the fund was written to the database, False if it wasnt
        """

        base_date = datetime.datetime.strptime("1999-01-01", "%Y-%m-%d")

        if not isinstance(fund_metadata, dict) or not isinstance(fund_weights, list):
            raise Exception(
                "Type mismatch. fund_metadata must be dictionary, fund_weights must be 2d list")

        cursor = self.db.cursor()
        portfolio_insertion = "INSERT INTO `portfolios`(`id`, `pid`, `issuer`, `issuer_id`, `name`, `symbol`, `structure`, `expense_ratio`, `risk`, `inception`, `type`) VALUES(NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        weight_insertion = "INSERT INTO `portfolio_weights`(`id`, `pid`, `last_updated`, `ticker`, `shares_held`, `market_value`, `percent_weight`) VALUES (NULL, %s, NULL, %s, %s, %s, %s);"
        flag_insertion = "INSERT INTO `portfolio_flags`(`id`, `pid`, `is_etf`, `is_vlip`, `is_vvap`, `is_529`, `is_active`, `is_closed`, `is_closed_to_new_investors`, `is_fund_of_funds`, `is_mscii_indexed_fund`, `is_index`, `is_bond`, `is_balanced`, `is_stock`, `is_international`, `is_market_neutral_fund`, `is_international_stock_fund`, `is_international_balanced_fund`, `is_domestic_stock_fund`, `is_taxable`, `is_tax_exempt`, `is_tax_managed`, `is_taxable_bond_fund`, `is_tax_exempt_bond_fund`, `is_tax_exempt_money_market_fund`, `is_tax_sensitive_fund`, `is_specialty_stock_fund`, `is_hybrid_fund`, `is_global`) VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

        pid = uuid.uuid4().hex
        f = fund_metadata["flags"]

        # make sure that the datestring of some of the older funds isnt <= 1970 (start of unix time)
        # if it is, convert to 1970-01-01
        if fund_metadata["inception"] < base_date:
            inception_date = base_date
        else:
            inception_date = fund_metadata["inception"]

        cursor.execute(portfolio_insertion, (pid, fund_metadata["issuer"], fund_metadata["issuer_id"], fund_metadata["name"], fund_metadata["ticker"], fund_metadata[
                       "category"], fund_metadata["expense_ratio"], fund_metadata["risk"], inception_date, fund_metadata["type"]))

        cursor.execute(flag_insertion, (pid, f["is_etf"], f["is_vlip"], f["is_vvap"], f["is_529"], f["isActiveFund"], f["isClosed"], f["isClosedToNewInvestors"], f["isFundOfFunds"], f["isMSCIIndexedFund"], f["isIndex"], f["isBond"], f["isBalanced"], f["isStock"], f["isInternational"], f["isMarketNeutralFund"],
                       f["isInternationalStockFund"], f["isInternationalBalancedFund"], f["isDomesticStockFund"], f["isTaxable"], f["isTaxExempt"], f["isTaxManaged"], f["isTaxableBondFund"], f["isTaxExemptBondFund"], f["isTaxExemptMoneyMarketFund"], f["isTaxSensitiveFund"], f["isSpecialtyStockFund"], f["isHybridFund"], f["isGlobal"]))

        for ticker in fund_weights:
            cursor.execute(weight_insertion,
                           (pid, ticker[0], ticker[1], ticker[2], ticker[3]))

        return pid

    def update_portfolio_price(self, has_check=False, **kwargs):
        """
        Takes a number of kwargs to be inserted into the database.

        Required Kwargs:
            - has_check (bool|False) :: Defaults to False. If True, an expensive SELECT query will be
                                        run for each portfolio price it attempts to update to avoid a
                                        duplicate price entry error
            - pid (str):: The portfolio ID
            - update_type (int):: The frequency type:
                                    0 - Updates every minute(if available)
                                    1 - Updates every 24 hours
                                    2 - Updates every 5 trading days
                                    3 - Updates every 20 trading days
            - current_price (float):: The current adjusted close price of the portfolio
            - last_price (float):: The last adjusted close price of the portfolio
            - price_change (float):: The change in the price from the last_price to the current_price
            - current_volume (int): : The current volume
            - last_volume (int):: The last volume
            - volume_change (float):: The change in the price of the last_volume to the current_volume

        Optional Kwargs:
            - created_on (datetime) :: The time the price was taken/created on
            - current_vol (float):: The current volatility
            - last_vol (float):: The last volatility
            - vol_change (float):: The change from the last_vol to the current_vol
            - delta_1d
            - delta_5d
            - delta_10d
            - delta_20d
            - delta_120d
            - delta_240d

        Returns:
            - True if success, False if an exception has occured
        """

        required_kwargs = ["pid", "update_type", "current_price", "last_price",
                           "price_change", "current_volume", "last_volume", "volume_change"]
        optional_kwargs = ["created_on", "current_vol", "last_vol", "vol_change", "delta_1d",
                           "delta_5d", "delta_10d", "delta_20d", "delta_120d", "delta_240d"]

        kwarg_keys = list(kwargs.keys())
        args = {}
        for req in required_kwargs:
            if req not in kwarg_keys:
                raise ValueError(
                    "Kwargs missing required key `{}`".format(req))
            args[req] = kwargs[req]

        for opt in optional_kwargs:
            if opt in kwarg_keys:
                args[opt] = kwargs[opt]
            else:
                args[opt] = False

        cursor = self.db.cursor()
        if has_check:
            check_query = "SELECT `id` FROM `portfolio_indicators` WHERE `created_on` = %s AND `pid` = %s LIMIT 1;"
            results = cursor.execute(
                check_query, (args["created_on"], args["pid"]))
            if len(results) != 0:
                return False

        insertion_query = "INSERT INTO `portfolio_indicators` (`id`, `pid`, `type`, `created_on`, `current_price`, `last_price`, `price_change`, `last_vol`, `current_vol`, `vol_change`, `current_volume`, `last_volume`, `volume_change`, `delta_1d`, `delta_5d`, `delta_10d`, `delta_20d`, `delta_120d`, `delta_240d`) VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        try:
            cursor.execute(insertion_query, (args["pid"], args["update_type"], args["created_on"], args["current_price"], args["last_price"], args["price_change"], args["last_vol"], args["current_vol"], args["vol_change"], args["current_volume"],
                                             args["last_volume"], args["volume_change"], args["delta_1d"], args["delta_5d"], args["delta_10d"], args["delta_20d"], args["delta_120d"], args["delta_240d"]))
        except Exception as e:
            print(e)
            cursor.close()
            return False
        cursor.close()
        return True

    def last_portfolio_price(self, pid, prices_to_return=1):
        """
        Gets the last portfolio prices given the portfolios PID

        Parameters:
            - pid (str) :: The pid of the portfolio
            - prices_to_return (int) :: How many prices to return

        Returns:
            - prices (list) :: A list of dicts with each dict having the keys : "pid", "created_on", "current_price"
        """

        select_query = "SELECT `pid`, `created_on`, `current_price` FROM `portfolio_indicators` WHERE `pid` = %s ORDER BY `created_on` DESC LIMIT %s;"
        cursor = self.db.cursor()
        cursor.execute(select_query, (pid, prices_to_return))
        results = cursor.fetchall()
        return results

    def get_stock_prices(self, tickers, datetime_from, datetime_to, granular=True):
        """
        Fetches the stock prices from the stock_data table and the live_stock_data table

        Parameters:
            - tickers (list):: The list of tickers
            - datetime_from (datetime):: From the datetime to get the stock prices from
            - datetime_to (datetime):: To what datetime to fetch the stock prices from
            - granular (bool):: If True, the live_stock_data table will be used, if False, the stock_data table will be used

        Returns:
            - results (list):: A list of dicts with the following keys:
                                    {
                                        "timestamp": datetime,
                                        "ticker": str,
                                        "open": float,
                                        "high": float,
                                        "low": float,
                                        "close": float
                                    }
        """

        if not isinstance(tickers, list):
            return False

        cursor = self.db.cursor()

        if granular:
            fetch_sql = "SELECT `ticker`, `timestamp`, `open`, `ask`, `close`, `volume` FROM `live_stock_data` WHERE `ticker` = %s"

        else:
            fetch_sql = "SELECT `ticker`, `date`, `open`, `high`, `low`, `close`, `volume` FROM `stock_data` WHERE `ticker` = %s "

        results = []

        if datetime_from:
            datetime_from = datetime_from.astimezone(pytz.utc)

        if datetime_to:
            datetime_to = datetime_to.astimezone(pytz.utc)

        if not datetime_from:
            datetime_from = datetime.datetime.now().replace(hour=0, minute=0, second=0,
                                                            microsecond=0) - datetime.timedelta(days=5)

        if not datetime_to:
            datetime_to = datetime.datetime.now().replace(
                hour=0, minute=0, second=0, microsecond=0)

        fetch_sql += " AND `timestamp` BETWEEN %s AND %s" if granular else "AND `date` BETWEEN %s AND %s"
        fetch_sql += " ORDER BY `{}` DESC;".format(
            "timestamp" if granular else "date")
        for tick in tickers:
            results = results + \
                self.q_append(cursor, fetch_sql, tick,
                              datetime_from, datetime_to)

        return results

    def most_recent_close(self, tickers, day_delta=0):
        """
        Takes a list of tickers and gets their last close price efficiently

        Parameters:
            - tickers(list):: A list of tickers
            - day_delta(int):: Optional, the number of days to subtract from the most recent day (when doing deltas/vols)
        """
        cursor = self.db.cursor()
        date_sql = "SELECT `date` FROM `stock_data` ORDER BY `date` DESC LIMIT 1;"
        cursor.execute(date_sql)
        date_res = cursor.fetchall()
        most_recent_date = date_res[0]["date"]

        if day_delta != 0:
            most_recent_date = most_recent_date - \
                datetime.timedelta(days=day_delta)

        filler = ["%s" for _ in range(tickers)]
        close_sql = "SELECT `ticker`, `close`, `volume`, FROM `stock_data` WHERE `ticker` IN ({}) AND `date` = %s;".format(
            ",".join(filler))

        results = self.q_append(cursor, close_sql, *tickers, most_recent_date)
        return results

    def most_recent_live_close(self, tickers):
        """
        Same as above but on the live granular stock data table
        """
        return False

    def resolve_pid(self, pid):
        """
        Takes a portfolio ID and resolves it, returning the full name and symbol

        Parameters:
            - pid (str):: The pid

        Returns:
            - resolved (dict):: Dict with the keys "name", "symbol", "issuer", "issuer_id"
        """
        resolve_query = "SELECT `name`, `symbol`, `issuer`, `issuer_id` FROM `portfolios` WHERE pid = %s;"
        cursor = self.db.cursor()
        cursor.execute(resolve_query, pid)
        results = cursor.fetchall()
        if not results:
            return {}
        return results[0]

    def resolve_issuer_id(self, issuer_id):
        """
        Resolves the issuer id (Vanguard Fund ID, Blackrock iShares ID, etc.) returning the pid

        Returns:
            - resolved (dict) :: Dict with the keys "name", "pid", "issuer"
        """
        resolve_query = "SELECT `name`, `pid`, `symbol`, `issuer` FROM `portfolios` WHERE `issuer_id` = %s;"
        cursor = self.db.cursor()
        cursor.execute(resolve_query, issuer_id)
        results = cursor.fetchall()
        if not results:
            return False
        return results[0]

    def get_pid_list(self):
        """
        Gets every unique pid from the portfolios table
        """

        cursor = self.db.cursor()
        pid_query = "SELECT `pid` FROM `portfolios`;"
        cursor.execute(pid_query)
        results = cursor.fetchall()
        pid_list = [x["pid"] for x in results]
        return pid_list

    def de_duplicate(self):
        """
        de duplicates the db
        """
        cursor = self.db.cursor()
        pid_query = "SELECT `pid` FROM `portfolios`;"
        price_date_query = "SELECT `id`, `created_on` FROM `portfolio_indicators` WHERE `pid` = %s ORDER BY `created_on` DESC;"

        cursor.execute(pid_query)
        pids = cursor.fetchall()
        if not pids:
            return False

        for pid in pids:
            pid = pid["pid"]
            cursor.execute(price_date_query, pid)
            results = cursor.fetchall()
            if not results:
                continue
            unique_dates = []
            to_delete = []
            for result in results:
                if result["created_on"] in unique_dates:
                    to_delete.append(result["id"])
                    continue
                else:
                    unique_dates.append(result["created_on"])
            if not to_delete:
                print("No duplicates")
                continue

            print("Deleting {} duplicate entries for {}".format(
                len(to_delete), pid))
            delete_query = "DELETE FROM `portfolio_indicators` WHERE `id` = %s;"
            for item in to_delete:
                cursor.execute(delete_query, item)
        return True

    def q_append(self, cursor, query, *args):
        """
        Casts a query onto a list of items
        """
        results = []
        for _ in range(len(args)):
            cursor.execute(query, args)
            fetched_results = cursor.fetchall()
            if fetched_results:
                results += fetched_results
        return results
