import os
from typing import Optional
import fitz
from logger import logger


def convert_pdf_to_images(pdf_path: str, out_dir: str) -> Optional[list[str]]:
    pdf_filename = os.path.splitext(os.path.basename(pdf_path))[0]

    pdf_out_dir = os.path.join(out_dir, pdf_filename)
    os.makedirs(pdf_out_dir, exist_ok=True)
    images_names = []
    try:
        pdf = fitz.open(pdf_path)

        for page_num, page in enumerate(pdf):
            pix = page.get_pixmap(dpi=300)
            image_name = f"{pdf_filename}_page_{page_num+1}.png"
            output_path = os.path.join(pdf_out_dir, image_name)
            pix.save(output_path)
            images_names.append(image_name)

        return images_names

    except Exception as e:
        logger.error(f"Error converting {pdf_path}: {str(e)}")
        return None
    finally:
        # Make sure to close the PDF
        if "pdf" in locals():
            pdf.close()
