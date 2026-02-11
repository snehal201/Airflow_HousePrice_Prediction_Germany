import streamlit as st
import pandas as pd
import joblib
import json
import os

# Page Config
st.set_page_config(page_title="German Rent Predictor", layout="centered")


# --- Load Model & Metrics ---
@st.cache_resource
def load_data():
    model_path = 'model/housing_model.pkl'
    metrics_path = 'model/metrics.json'

    if not os.path.exists(model_path):
        return None, None

    model = joblib.load(model_path)

    metrics = {"r2_score": 0}
    if os.path.exists(metrics_path):
        with open(metrics_path, 'r') as f:
            metrics = json.load(f)

    return model, metrics


model, metrics = load_data()

# --- App Header ---
st.title("🏠 Germany Rental Price Predictor")
st.markdown("Predict the **Total Rent** of an apartment based on its features.")

if model is None:
    st.error("Model file not found! Please run `python train_model.py` first.")
    st.stop()

# --- Display Model Accuracy ---
if metrics:
    accuracy = metrics.get("r2_score", 0) * 100

# --- Input Form ---
with st.form("prediction_form"):
    st.subheader("Enter Apartment Details")

    col1, col2 = st.columns(2)

    with col1:
        living_space = st.number_input("Living Space (m²)", min_value=10.0, max_value=500.0, value=60.0)

        # FIX: step=1 ensures this is an Integer, no decimals allowed
        no_rooms = st.number_input("Number of Rooms", min_value=1, max_value=10, value=3, step=1)

        # FIX: step=1 for Year as well
        year_constructed = st.number_input("Year Constructed", min_value=1900, max_value=2025, value=2015, step=1)

    with col2:
        states = [
            'Nordrhein_Westfalen', 'Sachsen', 'Bremen', 'Bayern', 'Berlin',
            'Hessen', 'Hamburg', 'Baden_Wuerttemberg', 'Thueringen', 'Sachsen_Anhalt', 'Other'
        ]
        heating_types = [
            'central_heating', 'floor_heating', 'district_heating',
            'gas_heating', 'oil_heating', 'self_contained_central_heating'
        ]

        selected_state = st.selectbox("State (Bundesland)", states)
        selected_heating = st.selectbox("Heating Type", heating_types)

    st.markdown("---")
    col3, col4 = st.columns(2)
    with col3:
        has_balcony = st.checkbox("Has Balcony", value=True)
    with col4:
        is_newly_built = st.checkbox("Newly Constructed", value=False)

    st.markdown("---")
    st.subheader("Compare with Actual Value (Optional)")
    # New Input: Actual Rent for comparison
    actual_rent_input = st.number_input("Known/Actual Rent (€)", min_value=0.0, value=0.0, step=10.0,
                                        help="Enter the actual rent if known to compare with the prediction.")

    submit_button = st.form_submit_button("Predict Rent 💶")

# --- Prediction Logic ---
if submit_button:
    # Prepare input data
    input_data = pd.DataFrame({
        'livingSpace': [living_space],
        'noRooms': [no_rooms],
        'heatingType': [selected_heating],
        'balcony': [1.0 if has_balcony else 0.0],
        'newlyConst': [1.0 if is_newly_built else 0.0],
        'yearConstructed': [year_constructed],
        'state': [selected_state]
    })

    try:
        prediction = model.predict(input_data)[0]

        # Display Result
        st.success(f"### Estimated Total Rent: €{prediction:,.2f}")

        # Comparison Logic
        if actual_rent_input > 0:
            diff = prediction - actual_rent_input

            col_a, col_b, col_c = st.columns(3)
            col_a.metric("Model Prediction", f"€{prediction:,.0f}")
            col_b.metric("Your Input", f"€{actual_rent_input:,.0f}")

            # Color logic for difference
            if abs(diff) < 50:
                col_c.metric("Difference", f"€{diff:,.0f}", delta_color="off")
                st.caption("✅ The prediction is very close!")
            else:
                # If prediction is higher than actual, it shows red (overpriced estimate)
                # If prediction is lower than actual, it shows green (good deal?)
                col_c.metric("Difference", f"€{diff:,.0f}", delta=-diff)

    except Exception as e:
        st.error(f"Prediction Error: {e}")

# --- Disclaimer ---
st.warning(
    "⚠️ **Note:** These predictions are based on a specific historical dataset (ImmoScout24). "
    "Real-world market prices may vary due to location specifics, inflation, and current demand. "
    "This tool should be used for estimation purposes only."
)