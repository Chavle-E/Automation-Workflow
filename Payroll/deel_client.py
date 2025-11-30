import requests
import logging
from typing import Optional, List, Dict
from ratelimit import limits, sleep_and_retry


class DeelClient:
    def __init__(self, api_key: str):
        self.base_url = "https://api.letsdeel.com/rest/v2"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "accept": "application/json",
            "content-type": "application/json"
        }

    @sleep_and_retry
    @limits(calls=5, period=1)  # 5 calls per second
    def get_all_contracts(self, contract_type: str = 'pay_as_you_go_time_based') -> List[Dict]:
        """
        Fetch all contracts from Deel API with pagination.

        Args:
            contract_type: Filter by contract type (default: pay_as_you_go_time_based)

        Returns:
            List of all matching contracts
        """
        all_contracts = []
        after_cursor = None
        url = f"{self.base_url}/contracts"

        while True:
            params = {'after_cursor': after_cursor} if after_cursor else {}

            try:
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                data = response.json()

                if 'data' in data:
                    contracts = [
                        contract for contract in data['data']
                        if contract['type'] == contract_type
                    ]
                    all_contracts.extend(contracts)

                # Check if there are more pages
                if not data.get('data') or not data.get('page', {}).get('cursor'):
                    break

                after_cursor = data['page']['cursor']

            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching Deel contracts: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    logging.error(f"Response: {e.response.content}")
                break

        logging.info(f"Fetched {len(all_contracts)} Deel contracts")
        return all_contracts

    @sleep_and_retry
    @limits(calls=5, period=1)
    def set_external_id(self, contract_id: str, harvest_user_id: str) -> bool:
        """Set Harvest user ID as external_id on Deel contract."""
        url = f"{self.base_url}/contracts/{contract_id}"
        payload = {
            "data": {
                "external_id": f"harvest_{harvest_user_id}"
            }
        }

        try:
            response = requests.patch(url, json=payload, headers=self.headers)
            response.raise_for_status()
            logging.info(f"Set external_id for contract {contract_id} to harvest_{harvest_user_id}")
            return True
        except requests.exceptions.RequestException as e:
            logging.error(f"Error setting external_id for contract {contract_id}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logging.error(f"Response: {e.response.content}")
            return False

    @sleep_and_retry
    @limits(calls=5, period=1)
    def find_contract_by_external_id(self, harvest_user_id: str) -> Optional[Dict]:
        """
        Find Deel contract by Harvest user ID stored in external_id.

        Args:
            harvest_user_id: Harvest user ID to search for

        Returns:
            Contract dict if found, None otherwise
        """
        url = f"{self.base_url}/contracts"
        params = {"external_id": f"harvest_{harvest_user_id}"}

        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            contracts = response.json().get("data", [])
            return contracts[0] if contracts else None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error finding contract by external_id: {e}")
            return None

    @sleep_and_retry
    @limits(calls=5, period=1)
    def submit_timesheet(self, contract_id: str, hours: float, date: str, description: str = "Uploaded") -> bool:
        """
        Submit timesheet to Deel API.

        Args:
            contract_id: Deel contract ID
            hours: Number of hours
            date: Date in YYYY-MM-DD format
            description: Timesheet description

        Returns:
            True if successful, False otherwise
        """
        url = f"{self.base_url}/timesheets"
        payload = {
            "data": {
                "contract_id": contract_id,
                "description": description,
                "date_submitted": date,
                "quantity": hours
            }
        }

        try:
            response = requests.post(url, json=payload, headers=self.headers)
            response.raise_for_status()
            logging.info(f"Timesheet submitted for contract {contract_id}: {hours} hours on {date}")
            return True
        except requests.exceptions.RequestException as e:
            error_message = "Unknown error"
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    error_message = error_data.get('errors', [{}])[0].get('message', 'Unknown error')
                except:
                    error_message = e.response.content

            logging.error(f"Error submitting timesheet for contract {contract_id}: {error_message}")
            return False