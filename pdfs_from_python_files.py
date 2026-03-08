import os
from pathlib import Path
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Preformatted
from reportlab.lib.enums import TA_LEFT


def create_pdf_from_python_file(py_file_path, output_dir):
    """
    Create a PDF from a Python file, showing the file name, path, and contents.

    Args:
        py_file_path: Path to the Python file
        output_dir: Directory where the PDF will be saved
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Generate PDF filename based on Python file path
    # Replace path separators with underscores
    relative_path = str(py_file_path).replace(os.sep, '_')
    pdf_filename = relative_path.replace('.py', '.pdf')
    pdf_path = os.path.join(output_dir, pdf_filename)

    # Create PDF document
    doc = SimpleDocTemplate(pdf_path, pagesize=letter,
                            topMargin=0.75 * inch, bottomMargin=0.75 * inch,
                            leftMargin=0.75 * inch, rightMargin=0.75 * inch)

    # Container for the 'Flowable' objects
    elements = []

    # Define styles
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=16,
        textColor='darkblue',
        spaceAfter=12
    )

    path_style = ParagraphStyle(
        'PathStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor='gray',
        spaceAfter=20
    )

    code_style = ParagraphStyle(
        'CodeStyle',
        parent=styles['Code'],
        fontSize=9,
        leftIndent=0,
        fontName='Courier',
        alignment=TA_LEFT
    )

    # Add file name as title
    filename = os.path.basename(py_file_path)
    elements.append(Paragraph(f"<b>{filename}</b>", title_style))

    # Add full path
    full_path = os.path.abspath(py_file_path)
    elements.append(Paragraph(f"Path: {full_path}", path_style))

    elements.append(Spacer(1, 0.2 * inch))

    # Read and add Python file content
    try:
        with open(py_file_path, 'r', encoding='utf-8') as f:
            code_content = f.read()

        # Use Preformatted to preserve code formatting
        code_para = Preformatted(code_content, code_style)
        elements.append(code_para)

    except Exception as e:
        error_text = f"Error reading file: {str(e)}"
        elements.append(Paragraph(error_text, styles['Normal']))

    # Build PDF
    try:
        doc.build(elements)
        print(f"Created PDF: {pdf_path}")
    except Exception as e:
        print(f"Error creating PDF for {py_file_path}: {str(e)}")


def find_and_convert_python_files(root_directory, output_dir='python_pdfs'):
    """
    Recursively find all .py files in a directory and convert them to PDFs.

    Args:
        root_directory: Root directory to search for Python files
        output_dir: Directory where PDFs will be saved (default: 'python_pdfs')
    """
    root_path = Path(root_directory)

    if not root_path.exists():
        print(f"Error: Directory '{root_directory}' does not exist.")
        return

    # Find all .py files recursively
    py_files = list(root_path.rglob('*.py'))

    if not py_files:
        print(f"No Python files found in '{root_directory}'")
        return

    print(f"Found {len(py_files)} Python file(s). Converting to PDF...\n")

    for py_file in py_files:
        try:
            create_pdf_from_python_file(py_file, output_dir)
        except Exception as e:
            print(f"Failed to process {py_file}: {str(e)}")

    print(f"\nConversion complete! PDFs saved in '{output_dir}' directory.")


# Example usage
if __name__ == "__main__":
    # Change this to your target directory
    directory_to_scan = "gold"  # Current directory
    output_directory = "python_pdfs"  # Where to save PDFs

    find_and_convert_python_files(directory_to_scan, output_directory)
