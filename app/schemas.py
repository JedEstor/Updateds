from ninja import Schema
from typing import List, Optional


class CustomerPart(Schema):
    Partcode: str
    Partname: str

class MaterialIn(Schema):
    mat_partcode: str
    dim_qty: float
    loss_percent: Optional[float] = 10.0

class MaterialOut(Schema):
    mat_partcode: str
    mat_partname: str
    mat_maker: str
    unit: str
    dim_qty: float
    loss_percent: float
    total: float

class TEPCodeIn(Schema):
    tep_code: str

class TEPCodeOut(Schema):
    part_code: str
    tep_code: str
    materials: List[MaterialOut] = []

class CustomerIn(Schema):
    customer_name: str
    parts: Optional[List[CustomerPart]] = None

class CustomerOut(Schema):
    id: int
    customer_name: str
    parts: List[CustomerPart]

class CustomerFullOut(Schema):
    id: int
    customer_name: str
    parts: List[CustomerPart] = []
    tep_codes: List[TEPCodeOut] = []

class TEPNodeOut(Schema):
    TEP_Code: str
    Materials: List[MaterialOut] = []

class PartNodeOut(Schema):
    Partcode: str
    Partname: str
    TEP_Codes: List[TEPNodeOut] = []


    
class CustomerTreeOut(Schema):
    customer_name: str
    Customer_Part: List[PartNodeOut] = []

class MaterialListIn(Schema):
    mat_partcode: str
    mat_partname: str
    mat_maker: str
    unit: str

class MaterialListOut(Schema):
    mat_partcode: str
    mat_partname: str
    mat_maker: str
    unit: str
class CustomerPart(Schema):
    Partcode: str
    Partname: str


class MaterialIn(Schema):
    mat_partcode: str
    dim_qty: float
    loss_percent: Optional[float] = 10.0

class MaterialOut(Schema):
    mat_partcode: str
    mat_partname: str
    mat_maker: str
    unit: str
    dim_qty: float
    loss_percent: float
    total: float

class TEPCodeIn(Schema):
    tep_code: str

class TEPCodeOut(Schema):
    part_code: str
    tep_code: str
    materials: List[MaterialOut] = []

class CustomerIn(Schema):
    customer_name: str
    parts: Optional[List[CustomerPart]] = None

class CustomerOut(Schema):
    id: int
    customer_name: str
    parts: List[CustomerPart]

class CustomerFullOut(Schema):
    id: int
    customer_name: str
    parts: List[CustomerPart] = []
    tep_codes: List[TEPCodeOut] = []

class TEPNodeOut(Schema):
    TEP_Code: str
    Materials: List[MaterialOut] = []

class PartNodeOut(Schema):
    Partcode: str
    Partname: str
    TEP_Codes: List[TEPNodeOut] = []


    
class CustomerTreeOut(Schema):
    customer_name: str
    Customer_Part: List[PartNodeOut] = []

class MaterialListIn(Schema):
    mat_partcode: str
    mat_partname: str
    mat_maker: str
    unit: str

class MaterialListOut(Schema):
    mat_partcode: str
    mat_partname: str
    mat_maker: str
    unit: str


# Forecast schemas
class MonthlyForecastIn(Schema):
    date: str                # e.g. "Jan-2026"
    unit_price: float = 0
    quantity: float = 0

class ForecastBatchPartIn(Schema):
    part_number: str
    part_name: str
    monthly_forecasts: List[MonthlyForecastIn]

class ForecastBatchIn(Schema):
    customer_name: str
    parts: List[ForecastBatchPartIn]


class ForecastIn(Schema):
    customer_name: Optional[str] = None
    part_number: str
    part_name: str
    monthly_forecasts: Optional[List[MonthlyForecastIn]] = None


class MonthlyForecastOut(Schema):
    date: str
    unit_price: float
    quantity: float
    Total_Quantity: str = "auto"
    Total_Amount: str = "auto"


class ForecastOut(Schema):
    id: int
    part_number: str
    part_name: str
    monthly_forecasts: List[dict] 


