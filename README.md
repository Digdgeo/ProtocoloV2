# 🤖 Protocolo V2 

Hi There! 👋

Welcome to **Protocolo V2**—the second official (though technically the fifth!) version of the Landsat image normalization process based on **Pseudo Invariant Areas (PIAs)**. This project aims to automate the processing of Landsat data and generate derived products such as NDVI, inundation masks, turbidity maps, and more.

While we're just getting started (this is still a private repo), we have some exciting things in the works! So stay tuned for updates.

## What's Inside

This repo is centered around three main classes:
- **`Landsat`**: Handles the normalization and processing of Landsat images.
- **`Products`**: Generates the derived products such as NDVI, water turbidity, and flood masks.
- **`Coast`**: Extracts the coastline (wet/dry line) from flood masks within the Espacio Natural de Doñana and assigns each line the corresponding sea level height from the Bonanza tide gauge.

All the magic is orchestrated and controlled by `download.py`, which automates the entire workflow.

## Features

- Normalize Landsat images using **PIAs** (Pseudo Invariant Areas).
- Generate various environmental products such as NDVI, flood masks, and turbidity maps.
- Extract and store coastline lines associated with tide levels.
- Process multiple scenes automatically and store results in a MongoDB database.
- Hydroperiod calculation for wetland management.

## Usage

1. **Normalization Process**: The main script handles Landsat normalization using PIAs.
2. **Product Generation**: Use the `Products` class to generate NDVI, turbidity, and other key products.
3. **Coastline Extraction**: Use the `Coast` class to extract the wet/dry boundary and assign tide gauge levels.
4. **Data Handling**: All job control and automation are managed by `download.py`.

## Installation

1. Clone this repository:
    ```bash
    git clone https://your-repo-url.git
    cd your-repo-folder
    ```
2. Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Configure your environment (e.g., MongoDB connection, paths to Landsat images).

4. Run the processing scripts.

## To Do List 📝

- [x] Include script for executing hydroperiod (data preparation) (done! ✅)
- [ ] Make the Docs (coming soon! 📖)
- [ ] Create a video tutorial (🎥)
- [x] Check old and new water masks (six checked) (done! ✅)
- [x] Add hydroperiod collection to database and enable filtering by clouds in preparation (done! ✅)
- [x] **Important**: Create `lunch scripts` using `argparse` to run them from `crontab`
- [ ] Make this a python package where reference image and PIAs are parameters of the class

## Contributing

We welcome contributions! Please fork the repo and submit a pull request, or open an issue to discuss any changes.

1. Fork the Project.
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`).
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`).
4. Push to the Branch (`git push origin feature/AmazingFeature`).
5. Open a Pull Request.

## License

Distributed under the MIT License. See `LICENSE` for more information.

---

🚀 **Protocolo V2** is under continuous development. Stay tuned for more updates as we keep improving the process and adding more features!
