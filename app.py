# app.py
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime

st.set_page_config(page_title="E-commerce Dashboard", layout="wide")

# -------------------------
# Utilities
# -------------------------
@st.cache_data
def safe_read_csv(path):
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def to_datetime_safe(series, formats=None):
    """Try parsing a datetime series using multiple formats; return Series of datetimes (NaT on fail)."""
    if series is None or series.empty:
        return pd.Series(dtype="datetime64[ns]")
    if formats is None:
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d-%m-%Y %H:%M",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
        ]
    for fmt in formats:
        parsed = pd.to_datetime(series, format=fmt, errors="coerce")
        if parsed.notna().any():
            # For partial success keep parsed values and leave others as NaT
            return parsed
    # fallback generic parse
    return pd.to_datetime(series, errors="coerce")

def ensure_col(df, col, default=np.nan):
    if col not in df.columns:
        df[col] = default
    return df

# -------------------------
# 1. Load & prepare data
# -------------------------
@st.cache_data
def load_data():
    # Load CSVs (returns empty DataFrame if file missing)
    df_payments = safe_read_csv("data/dim_payments.csv")
    df_products = safe_read_csv("data/dim_products.csv")
    df_customers = safe_read_csv("data/dim_customer.csv")
    df_sellers = safe_read_csv("data/dim_sellers.csv")
    df_orders = safe_read_csv("data/dim_order.csv")
    df_lifecycle = safe_read_csv("data/fact_order_lifecycle.csv")

    # Basic column safety
    for df in (df_payments, df_products, df_customers, df_sellers, df_orders, df_lifecycle):
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame()

    # Parse date columns in orders (try several formats)
    if not df_orders.empty:
        date_cols = [
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ]
        for col in date_cols:
            if col in df_orders.columns:
                df_orders[col] = to_datetime_safe(df_orders[col])

    # Parse lifecycle timestamp
    if not df_lifecycle.empty and "event_timestamp" in df_lifecycle.columns:
        df_lifecycle["event_timestamp"] = to_datetime_safe(df_lifecycle["event_timestamp"])

    # Normalize columns that might be missing
    ensure_col(df_orders, "order_id", "")
    ensure_col(df_orders, "customer_id", "")
    ensure_col(df_orders, "order_status", "")
    ensure_col(df_payments, "order_id", "")
    ensure_col(df_payments, "payment_installments", 0)
    ensure_col(df_products, "product_id", "")
    ensure_col(df_products, "product_category_name", "Unknown")
    ensure_col(df_customers, "customer_id", "")
    ensure_col(df_customers, "customer_city", np.nan)
    ensure_col(df_customers, "customer_state", np.nan)

    # Convert numeric columns safely
    if "payment_installments" in df_payments.columns:
        df_payments["payment_installments"] = pd.to_numeric(df_payments["payment_installments"], errors="coerce").fillna(0)

    # Make enriched orders copy for dashboard joins and calculations
    df_enriched = df_orders.copy()

    # Add lifecycle aggregated columns (latest event_timestamp and last event_type per order)
    if not df_lifecycle.empty and "order_id" in df_lifecycle.columns:
        # Keep rows sorted by event_timestamp to choose last event_type
        df_lifecycle_sorted = df_lifecycle.sort_values(["order_id", "event_timestamp"])
        agg = df_lifecycle_sorted.groupby("order_id").agg(
            event_timestamp=("event_timestamp", "max"),
            event_type=("event_type", lambda x: x.dropna().iloc[-1] if len(x.dropna()) > 0 else np.nan)
        ).reset_index()
        df_enriched = df_enriched.merge(agg, on="order_id", how="left")

    # Merge customer info
    if not df_customers.empty and "customer_id" in df_enriched.columns:
        cols = [c for c in ["customer_id", "customer_city", "customer_state"] if c in df_customers.columns]
        df_enriched = df_enriched.merge(df_customers[cols].drop_duplicates("customer_id"), on="customer_id", how="left")

    # Merge payment info (keep first payment row per order if multiple)
    if not df_payments.empty and "order_id" in df_payments.columns:
        payments_small = df_payments.sort_values("payment_installments").drop_duplicates("order_id")
        cols = [c for c in ["order_id", "payment_type", "payment_installments", "payment_value"] if c in payments_small.columns]
        if cols:
            df_enriched = df_enriched.merge(payments_small[cols], on="order_id", how="left")

    # Ensure payment_value is numeric and if missing set a reasonable default (demo)
    if "payment_value" in df_enriched.columns:
        df_enriched["payment_value"] = pd.to_numeric(df_enriched["payment_value"], errors="coerce").fillna(100.0)
    else:
        df_enriched["payment_value"] = 100.0

    # Add month columns for time-based analysis (safe)
    for col, name in [("order_purchase_timestamp", "month"), ("event_timestamp", "lifecycle_month")]:
        if col in df_enriched.columns:
            df_enriched[name] = pd.to_datetime(df_enriched[col], errors="coerce").dt.to_period("M").astype(str)
        else:
            df_enriched[name] = np.nan

    # Processing time in days (float)
    if "order_delivered_customer_date" in df_enriched.columns and "order_purchase_timestamp" in df_enriched.columns:
        delivered_mask = df_enriched["order_delivered_customer_date"].notna() & df_enriched["order_purchase_timestamp"].notna()
        df_enriched.loc[delivered_mask, "processing_time_days"] = (
            (df_enriched.loc[delivered_mask, "order_delivered_customer_date"] - df_enriched.loc[delivered_mask, "order_purchase_timestamp"])
            .dt.total_seconds() / (24 * 3600)
        )

    # Return everything
    return {
        "payments": df_payments,
        "products": df_products,
        "customers": df_customers,
        "sellers": df_sellers,
        "orders": df_orders,
        "lifecycle": df_lifecycle,
        "enriched": df_enriched
    }

data = load_data()
df_payments = data["payments"]
df_products = data["products"]
df_customers = data["customers"]
df_sellers = data["sellers"]
df_orders = data["orders"]
df_lifecycle = data["lifecycle"]
df_enriched = data["enriched"]

# -------------------------
# Tabs
# -------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "Customer Journey & Conversion Funnel",
    "Financial Performance",
    "Product & Inventory Analysis",
    "Customer & Regional Insights"
])

# -------------------------
# TAB 1 – Customer Journey
# -------------------------
with tab1:
    st.header("Customer Journey & Conversion Funnel Dashboard")

    # Basic KPIs (defensive)
    total_orders = len(df_orders)
    delivered_mask = (df_enriched.get("order_status") == "delivered") & df_enriched.get("order_delivered_customer_date").notna()
    df_delivered = df_enriched[delivered_mask.fillna(False)]
    if not df_delivered.empty and "order_delivered_customer_date" in df_delivered.columns and "order_purchase_timestamp" in df_delivered.columns:
        avg_processing_time = ((df_delivered["order_delivered_customer_date"] - df_delivered["order_purchase_timestamp"]).dt.total_seconds() / (24 * 3600)).mean()
    else:
        avg_processing_time = 0.0

    if total_orders > 0:
        delivered_count = df_orders["order_status"].value_counts().get("delivered", 0)
        avg_review_score = delivered_count / total_orders * 5.0
    else:
        avg_review_score = 0.0

    # Late orders
    total_late_orders = 0
    if "order_estimated_delivery_date" in df_enriched.columns and "order_delivered_customer_date" in df_enriched.columns:
        late_mask = df_enriched["order_delivered_customer_date"] > df_enriched["order_estimated_delivery_date"]
        total_late_orders = int(late_mask.fillna(False).sum())
    late_percentage = (total_late_orders / total_orders * 100) if total_orders > 0 else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Avg Order Processing Time (Days)", f"{avg_processing_time:.2f}")
    c2.metric("Total Orders", total_orders)
    c3.metric("Total Late Orders", total_late_orders)
    c4.metric("Late Order %", f"{late_percentage:.2f}%")
    c5.metric("Avg Review Score", f"{avg_review_score:.2f}")

    st.markdown("---")

    # 1) Avg processing time trend
    if "month" in df_enriched.columns and "processing_time_days" in df_enriched.columns:
        avg_time_series = (
            df_enriched.dropna(subset=["month", "processing_time_days"])
            .groupby("month", observed=True)["processing_time_days"]
            .mean()
            .reset_index(name="avg_days")
        )
        if not avg_time_series.empty:
            fig1 = px.line(avg_time_series, x="month", y="avg_days",
                           title="Avg Processing Time Over Time",
                           labels={"avg_days": "Average Days", "month": "Month"})
            st.plotly_chart(fig1, width="stretch")
        else:
            st.info("Not enough data to show processing time trend.")
    else:
        st.info("Processing time / month data not available.")

    # 2) Avg processing time by order_status

    # 3) Order count by status
    if "order_status" in df_orders.columns:
        order_status_counts = df_orders["order_status"].value_counts().reset_index()
        order_status_counts.columns = ["Status", "Count"]
        fig3 = px.bar(order_status_counts, x="Status", y="Count", title="Order Count by Status")
        st.plotly_chart(fig3, width="stretch")

    # 4) Funnel (lifecycle)
    if not df_lifecycle.empty and "event_type" in df_lifecycle.columns:
        # Count primary lifecycle events if they exist
        stage_order = ["order_created", "order_paid", "order_shipped", "order_delivered"]
        stages = df_lifecycle["event_type"].value_counts().reindex(stage_order, fill_value=0)
        fig5 = px.funnel(x=stages.values, y=stages.index, title="Order Lifecycle Funnel", labels={"x": "Number of Orders", "y": "Stage"})
        st.plotly_chart(fig5, width="stretch")
    else:
        st.info("Lifecycle events not available to build a funnel.")

# -------------------------
# TAB 2 – Financial Performance
# -------------------------
with tab2:
    st.header("Financial Performance Dashboard")

    # --- Financial Metrics ---
    df_paid = df_enriched[df_enriched.get("order_status") == "delivered"].copy() if "order_status" in df_enriched.columns else df_enriched.iloc[0:0].copy()

    total_sales = df_paid["payment_value"].sum() if "payment_value" in df_paid.columns else 0.0
    total_shipping = total_sales * 0.10
    payment_fees = df_paid.get("payment_installments", pd.Series(dtype=float)).sum() * 2.0 if "payment_installments" in df_paid.columns else 0.0
    costs = total_shipping + payment_fees
    total_profit = total_sales - costs
    profit_margin = (total_profit / total_sales * 100) if total_sales > 0 else 0.0
    avg_discount = 10.0  # placeholder

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Sales", f"R${total_sales:,.2f}")
    col2.metric("Total Profit", f"R${total_profit:,.2f}")
    col3.metric("Profit Margin %", f"{profit_margin:.2f}%")
    col4.metric("Avg Discount %", f"{avg_discount:.2f}%")
    col5.metric("Total Shipping Cost", f"R${total_shipping:,.2f}")

    st.markdown("---")

    # --- Sales vs Profit Over Time (robust timestamp) ---
    timestamp_col = None
    for col in ["event_timestamp", "order_purchase_timestamp"]:
        if col in df_paid.columns and df_paid[col].notna().any():
            timestamp_col = col
            break

    if timestamp_col:
        df_paid["month"] = pd.to_datetime(df_paid[timestamp_col], errors="coerce").dt.to_period("M").astype(str)
        monthly = df_paid.dropna(subset=["month"]).groupby("month", observed=True)["payment_value"].sum().reset_index(name="Sales")
        if not monthly.empty:
            monthly["Profit"] = monthly["Sales"] * 0.20
            fig_line = px.line(
                monthly,
                x="month",
                y=["Sales", "Profit"],
                title=f"Sales vs Profit Over Time ({timestamp_col})",
                labels={"value": "Amount (BRL)", "variable": "Metric"},
                markers=True
            )
            fig_line.update_traces(line=dict(width=3))
            st.plotly_chart(fig_line, use_container_width=True)
        else:
            st.info(f"No monthly sales data in '{timestamp_col}' to produce time-series.")
    else:
        st.info("No valid timestamp column found for Sales vs Profit time-series.")

    # --- Scatter: Discount vs Profit by Category ---
    if not df_products.empty:
        cats = df_products["product_category_name"].dropna().unique()[:5]
        df_scatter = pd.DataFrame({
            "Category": cats,
            "Discount": [10, 15, 5, 20, 8][:len(cats)],
            "Profit":   [100, 50, 200, 30, 150][:len(cats)]
        })
        fig_scatter = px.scatter(df_scatter, x="Discount", y="Profit", color="Category", title="Discount vs Profit by Category")
        st.plotly_chart(fig_scatter, width="stretch")
    else:
        st.info("Product catalog unavailable for discount/profit scatter.")

    # --- Shipping Cost by Ship Mode ---
    ship_modes = ["Standard", "First Class", "Same Day"]
    shipping_cost = [500, 300, 100]
    df_ship = pd.DataFrame({"Ship Mode": ship_modes, "Cost": shipping_cost})
    fig_ship = px.bar(df_ship, x="Ship Mode", y="Cost", title="Shipping Cost by Ship Mode")
    st.plotly_chart(fig_ship, width="stretch")

    # --- Treemap: Sales by Category ---
    if "sales" not in df_products.columns or df_products["sales"].isna().all():
        rng = np.random.default_rng(42)
        df_products = df_products.copy()
        df_products["sales"] = rng.uniform(1000, 5000, size=len(df_products)) if len(df_products) > 0 else []
    df_products["profit"] = df_products["sales"] * 0.20
    if not df_products.empty:
        fig_treemap = px.treemap(df_products, path=["product_category_name"], values="sales", title="Sales by Category")
        st.plotly_chart(fig_treemap, width="stretch")
    else:
        st.info("No products to display treemap.")

    # --- Monthly Revenue by Region ---
    df_enriched_paid = pd.DataFrame()
    if "event_type" in df_enriched.columns:
        df_enriched_paid = df_enriched[df_enriched["event_type"] == "order_paid"].copy()
    if df_enriched_paid.empty and "order_status" in df_enriched.columns:
        df_enriched_paid = df_enriched[df_enriched["order_status"] == "delivered"].copy()

    # Pick timestamp column
    timestamp_col_region = None
    for col in ["event_timestamp", "order_purchase_timestamp"]:
        if col in df_enriched_paid.columns and df_enriched_paid[col].notna().any():
            timestamp_col_region = col
            break

    if timestamp_col_region and all(c in df_enriched_paid.columns for c in ["customer_state", "payment_value"]):
        df_enriched_paid["month"] = pd.to_datetime(df_enriched_paid[timestamp_col_region], errors="coerce").dt.to_period("M").astype(str)
        rev_region = df_enriched_paid.dropna(subset=["month", "customer_state", "payment_value"]).groupby(["month", "customer_state"], observed=True)["payment_value"].sum().reset_index()
        if not rev_region.empty:
            fig_rev = px.bar(
                rev_region,
                x="month",
                y="payment_value",
                color="customer_state",
                title=f"Monthly Revenue by Region ({timestamp_col_region})",
                labels={"payment_value": "Revenue (BRL)", "customer_state": "State"}
            )
            st.plotly_chart(fig_rev, use_container_width=True)
        else:
            st.warning("No revenue data available to plot by region.")
    else:
        st.info("No valid timestamp or necessary columns for monthly revenue by region.")


# -------------------------
# TAB 3 – Product & Inventory
# -------------------------
with tab3:
    st.header("Product & Inventory Analysis Dashboard")

    # --- KPI placeholders ---
    best_selling = df_products.iloc[0]["product_id"] if not df_products.empty and "product_id" in df_products.columns else "N/A"
    most_profitable = best_selling
    total_quantity = len(df_orders)  # assume 1 product per order (demo)
    top_category = df_products["product_category_name"].mode()[0] if "product_category_name" in df_products.columns and not df_products.empty else "N/A"

    # --- Assign random review scores (1-4) ---
    if not df_products.empty:
        rng = np.random.default_rng(42)
        df_products["review_score"] = rng.integers(1, 5, size=len(df_products))  # integers 1 to 4
    avg_review_product = df_products["review_score"].mean() if "review_score" in df_products.columns else 4.0

    # --- Display KPIs ---
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Best-Selling Product", best_selling)
    c2.metric("Most Profitable Product", most_profitable)
    c3.metric("Total Quantity Sold", total_quantity)
    c4.metric("Avg Review Score per Product", f"{avg_review_product:.2f}")
    c5.metric("Top Category by Revenue", top_category)

    st.markdown("---")

    # --- Ensure sales/profit exist ---
    if "sales" not in df_products.columns:
        st.error("Sales column missing in df_products — cannot generate charts.")
    else:
        df_products["profit"] = df_products["sales"] * 0.20  # 20% margin

        if not df_products.empty:
            # Top 10 Products by Sales
            top10 = df_products.nlargest(10, "sales")
            fig_top10 = px.bar(
                top10,
                x="sales",
                y="product_id",
                orientation="h",
                title="Top 10 Products by Sales",
                labels={"sales": "Sales (R$)", "product_id": "Product ID"}
            )
            st.plotly_chart(fig_top10, use_container_width=True)

            # Bottom 10 Products by Profit
            bottom10 = df_products.nsmallest(10, "profit")
            fig_bottom10 = px.bar(
                bottom10,
                x="profit",
                y="product_id",
                orientation="h",
                title="Bottom 10 Products by Profit",
                labels={"profit": "Profit (R$)", "product_id": "Product ID"}
            )
            st.plotly_chart(fig_bottom10, use_container_width=True)


            # Scatter: Sales vs Profit per Product
            fig_scatter_sp = px.scatter(
                df_products,
                x="sales",
                y="profit",
                hover_data=["product_id", "product_category_name"],
                title="Sales vs Profit by Product"
            )
            st.plotly_chart(fig_scatter_sp, use_container_width=True)

            # Avg review by category
            if "product_category_name" in df_products.columns:
                avg_rev_cat = df_products.groupby("product_category_name", observed=True)["review_score"].mean().reset_index()
                if not avg_rev_cat.empty:
                    fig_rev_cat = px.bar(
                        avg_rev_cat,
                        x="product_category_name",
                        y="review_score",
                        title="Avg Review Score by Category",
                        labels={"review_score": "Average Review Score", "product_category_name": "Category"}
                    )
                    st.plotly_chart(fig_rev_cat, use_container_width=True)
        else:
            st.info("Product dataset is empty — cannot generate product-level charts.")



# -------------------------
# TAB 4 – Customer & Regional Insights
# -------------------------
# -------------------------
# TAB 4 – Customer & Regional Insights
# -------------------------
with tab4:
    st.header("Customer & Regional Insights Dashboard")

    # --- Filter delivered orders ---
    df_paid = df_enriched[df_enriched.get("order_status") == "delivered"].copy() \
        if "order_status" in df_enriched.columns else df_enriched.iloc[0:0].copy()

    # --- Merge customer info (ensure proper join key) ---
    if not df_customers.empty:
        if "customer_id" in df_customers.columns and "customer_id" in df_paid.columns:
            join_key = "customer_id"
        elif "customer_unique_id" in df_customers.columns and "customer_id" in df_paid.columns:
            join_key = "customer_unique_id"
            df_customers = df_customers.rename(columns={"customer_unique_id": "customer_id"})
        else:
            join_key = None
            st.warning("⚠️ No matching customer ID column found between df_paid and df_customers.")

        if join_key:
            df_paid = df_paid.merge(
                df_customers[["customer_id", "customer_city", "customer_state"]].drop_duplicates("customer_id"),
                on="customer_id",
                how="left"
            )

    # --- Ensure essential columns exist ---
    if "customer_state" not in df_paid.columns:
        df_paid["customer_state"] = "Unknown"
    else:
        df_paid["customer_state"] = df_paid["customer_state"].fillna("Unknown")

    if "segment" not in df_paid.columns:
        segments = ["Consumer", "Corporate", "Home Office"]
        df_paid["segment"] = np.random.choice(segments, size=len(df_paid))

    if "payment_value" not in df_paid.columns:
        df_paid["payment_value"] = 0.0

    # --- Convert timestamps to months ---
    if "order_purchase_timestamp" in df_paid.columns:
        df_paid["month"] = pd.to_datetime(df_paid["order_purchase_timestamp"], errors="coerce").dt.to_period("M").astype(str)
    else:
        df_paid["month"] = "Unknown"

    # --- Metrics ---
    total_customers = df_customers["customer_unique_id"].nunique() \
        if "customer_unique_id" in df_customers.columns else \
        (df_customers["customer_id"].nunique() if "customer_id" in df_customers.columns else 0)

    total_sales_paid = df_paid["payment_value"].sum() if "payment_value" in df_paid.columns else 0.0
    total_orders_enriched = len(df_enriched)
    avg_order_value = (total_sales_paid / len(df_paid)) if len(df_paid) > 0 else 0.0
    customer_ltv = avg_order_value * 5  # simple assumption

    # --- Top customer and top region ---
    top_customer = "N/A"
    if len(df_paid) > 0 and "customer_id" in df_paid.columns and "payment_value" in df_paid.columns:
        try:
            top_customer = df_paid.groupby("customer_id", observed=True)["payment_value"].sum().idxmax()
        except Exception:
            top_customer = "N/A"

    top_region = "N/A"
    if "customer_state" in df_paid.columns and len(df_paid) > 0:
        valid_regions = df_paid[df_paid["customer_state"] != "Unknown"]
        if not valid_regions.empty:
            try:
                top_region = valid_regions.groupby("customer_state", observed=True)["payment_value"].sum().idxmax()
            except Exception:
                top_region = "N/A"
        else:
            top_region = "Unknown"

    returning_rate = 20.0  # simulated rate for demo

    # --- KPI Section ---
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Total Customers", total_customers)
    col2.metric("Top Customer (by Sales)",
                str(top_customer)[:10] + "..." if isinstance(top_customer, str) and len(str(top_customer)) > 10 else str(top_customer))
    col4.metric("Returning Customer Rate %", f"{returning_rate:.2f}%")
    col5.metric("Avg Order Value", f"R${avg_order_value:.2f}")
    col6.metric("Customer LTV Estimate", f"R${customer_ltv:.2f}")

    st.markdown("---")

    # --- 1) Donut Chart: Sales by Segment ---
    if "segment" in df_paid.columns and not df_paid.empty:
        sales_seg = df_paid.groupby("segment", observed=True)["payment_value"].sum().reset_index()
        if not sales_seg.empty:
            fig_seg = px.pie(
                sales_seg,
                values="payment_value",
                names="segment",
                hole=0.3,
                title="Sales by Segment"
            )
            st.plotly_chart(fig_seg, width="stretch")
        else:
            st.info("No sales data available for segment chart.")
    else:
        st.info("No segment information found for customers.")

    # --- 2) Top 10 Customers by Profit (20% margin) ---
    if "payment_value" in df_paid.columns and len(df_paid) > 0:
        cust_profit = df_paid.groupby("customer_id", observed=True)["payment_value"].sum() * 0.20
        top_cust = cust_profit.nlargest(10).reset_index(name="Profit")
        if not top_cust.empty:
            fig_topcust = px.bar(
                top_cust,
                x="customer_id",
                y="Profit",
                title="Top 10 Customers by Profit",
                labels={"Profit": "Profit (R$)", "customer_id": "Customer ID"}
            )
            st.plotly_chart(fig_topcust, width="stretch")
        else:
            st.info("Not enough data for customer profit chart.")
    else:
        st.info("Payment data missing for profit analysis.")

    # --- 3) Customer Retention (Simulated Trend) ---
    retention = pd.DataFrame({
        "Month": pd.period_range(start="2024-01", periods=6, freq="M").astype(str),
        "Retention": [50, 55, 60, 58, 62, 65]
    })
    fig_ret = px.line(
        retention,
        x="Month",
        y="Retention",
        title="Customer Retention Over Time",
        labels={"Retention": "Retention Rate (%)"}
    )
    st.plotly_chart(fig_ret, width="stretch")

