"""
Data Wrangler - Desktop Application
A desktop application for cleaning and merging CSV files.
Created by Mohamad W.
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import pandas as pd
import tempfile
import os
from pathlib import Path
from typing import Dict, List, Optional
import threading

# Import our modules
from wrangle.cleaning import clean_dataframe, detect_encoding
from wrangle.merge import prepare_excel_export, validate_merge_operation
from wrangle.report import generate_quality_report, format_report_text, generate_processing_log
from wrangle.io import safe_read_csv, safe_write_excel, read_multiple_csvs, save_report_to_file


class DataWranglerApp:
    """Main application class for Data Wrangler desktop version."""
    
    def __init__(self, root):
        self.root = root
        self.root.title("Data Wrangler 🔧 - by Mohamad W.")
        self.root.geometry("1200x800")
        self.root.minsize(800, 600)
        
        # Set application icon
        self.set_icon()
        
        # Initialize variables
        self.uploaded_files = []
        self.processed_data = {}
        self.quality_report = None
        self.processing_log = []
        self.current_preview_index = 0
        self.preview_dataframes = {}
        
        # Create GUI
        self.create_widgets()
        
        # Configure style
        self.configure_style()
    
    def set_icon(self):
        """Set application icon."""
        try:
            # Try to set the custom icon
            if os.path.exists("data_wrangler.ico"):
                self.root.iconbitmap("data_wrangler.ico")
            else:
                # Fallback to default
                self.root.iconbitmap(default="")
        except Exception as e:
            # If icon setting fails, continue without it
            print(f"Could not set icon: {e}")
            pass
    
    def configure_style(self):
        """Configure the application style."""
        style = ttk.Style()
        style.theme_use('clam')
        
        # Configure colors
        style.configure('Title.TLabel', font=('Arial', 16, 'bold'), foreground='#1f77b4')
        style.configure('Header.TLabel', font=('Arial', 12, 'bold'), foreground='#2c3e50')
        style.configure('Success.TLabel', foreground='#28a745')
        style.configure('Warning.TLabel', foreground='#ffc107')
        style.configure('Error.TLabel', foreground='#dc3545')
    
    def create_widgets(self):
        """Create the main GUI widgets."""
        # Main container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(2, weight=1)
        
        # Title
        title_label = ttk.Label(main_frame, text="🔧 Data Wrangler", style='Title.TLabel')
        title_label.grid(row=0, column=0, columnspan=2, pady=(0, 20))
        
        # Left panel - Controls
        self.create_control_panel(main_frame)
        
        # Right panel - Data display
        self.create_data_panel(main_frame)
        
        # Status bar
        self.create_status_bar(main_frame)
    
    def create_control_panel(self, parent):
        """Create the control panel on the left."""
        control_frame = ttk.LabelFrame(parent, text="Controls", padding="10")
        control_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 10))
        
        # File upload section
        upload_frame = ttk.LabelFrame(control_frame, text="File Upload", padding="5")
        upload_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(upload_frame, text="📁 Select CSV Files", 
                  command=self.select_files).pack(fill=tk.X, pady=2)
        
        self.file_listbox = tk.Listbox(upload_frame, height=4)
        self.file_listbox.pack(fill=tk.X, pady=2)
        
        ttk.Button(upload_frame, text="🗑️ Clear Files", 
                  command=self.clear_files).pack(fill=tk.X, pady=2)
        
        # Processing options
        options_frame = ttk.LabelFrame(control_frame, text="Processing Options", padding="5")
        options_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Delimiter
        ttk.Label(options_frame, text="CSV Delimiter:").pack(anchor=tk.W)
        self.delimiter_var = tk.StringVar(value=",")
        delimiter_combo = ttk.Combobox(options_frame, textvariable=self.delimiter_var, 
                                     values=[",", ";", "\t", "|"], state="readonly")
        delimiter_combo.pack(fill=tk.X, pady=(0, 5))
        
        # Decimal separator
        ttk.Label(options_frame, text="Decimal Separator:").pack(anchor=tk.W)
        self.decimal_var = tk.StringVar(value=".")
        decimal_combo = ttk.Combobox(options_frame, textvariable=self.decimal_var, 
                                   values=[".", ","], state="readonly")
        decimal_combo.pack(fill=tk.X, pady=(0, 5))
        
        # Merge mode
        ttk.Label(options_frame, text="Merge Mode:").pack(anchor=tk.W)
        self.merge_mode_var = tk.StringVar(value="per_sheet")
        merge_combo = ttk.Combobox(options_frame, textvariable=self.merge_mode_var, 
                                 values=["per_sheet", "single_sheet"], state="readonly")
        merge_combo.pack(fill=tk.X, pady=(0, 5))
        
        # NA handling
        ttk.Label(options_frame, text="NA Handling:").pack(anchor=tk.W)
        self.na_policy_var = tk.StringVar(value="keep")
        na_combo = ttk.Combobox(options_frame, textvariable=self.na_policy_var, 
                              values=["keep", "drop", "fill"], state="readonly")
        na_combo.pack(fill=tk.X, pady=(0, 5))
        
        # Fill value
        ttk.Label(options_frame, text="Fill Value (if fill selected):").pack(anchor=tk.W)
        self.fill_value_var = tk.StringVar(value="")
        ttk.Entry(options_frame, textvariable=self.fill_value_var).pack(fill=tk.X, pady=(0, 5))
        
        # Date columns
        ttk.Label(options_frame, text="Date Columns (comma-separated):").pack(anchor=tk.W)
        self.date_columns_var = tk.StringVar(value="")
        ttk.Entry(options_frame, textvariable=self.date_columns_var).pack(fill=tk.X, pady=(0, 5))
        
        # Datetime columns
        ttk.Label(options_frame, text="Datetime Columns (comma-separated):").pack(anchor=tk.W)
        self.datetime_columns_var = tk.StringVar(value="")
        ttk.Entry(options_frame, textvariable=self.datetime_columns_var).pack(fill=tk.X, pady=(0, 5))
        
        # Datetime format
        ttk.Label(options_frame, text="Datetime Format:").pack(anchor=tk.W)
        self.datetime_format_var = tk.StringVar(value="%Y-%m-%d %H:%M:%S")
        datetime_format_combo = ttk.Combobox(options_frame, textvariable=self.datetime_format_var,
                                           values=["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", 
                                                  "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y"],
                                           state="readonly")
        datetime_format_combo.pack(fill=tk.X, pady=(0, 5))
        
        # Auto-detect datetime
        self.auto_detect_datetime_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(options_frame, text="Auto-detect Datetime Columns", 
                       variable=self.auto_detect_datetime_var).pack(anchor=tk.W, pady=(0, 5))
        
        # Advanced options
        advanced_frame = ttk.LabelFrame(control_frame, text="Advanced Options", padding="5")
        advanced_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.standardize_cols_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(advanced_frame, text="Standardize Column Names", 
                       variable=self.standardize_cols_var).pack(anchor=tk.W)
        
        self.infer_dtypes_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(advanced_frame, text="Infer Data Types", 
                       variable=self.infer_dtypes_var).pack(anchor=tk.W)
        
        self.trim_whitespace_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(advanced_frame, text="Trim Whitespace", 
                       variable=self.trim_whitespace_var).pack(anchor=tk.W)
        
        self.remove_duplicates_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(advanced_frame, text="Remove Duplicates", 
                       variable=self.remove_duplicates_var).pack(anchor=tk.W)
        
        # Action buttons
        action_frame = ttk.Frame(control_frame)
        action_frame.pack(fill=tk.X, pady=(10, 0))
        
        ttk.Button(action_frame, text="🔄 Process Files", 
                  command=self.process_files, style='Accent.TButton').pack(fill=tk.X, pady=2)
        
        ttk.Button(action_frame, text="📊 Download Excel", 
                  command=self.download_excel).pack(fill=tk.X, pady=2)
        
        ttk.Button(action_frame, text="📋 Download Report", 
                  command=self.download_report).pack(fill=tk.X, pady=2)
    
    def create_data_panel(self, parent):
        """Create the data display panel on the right."""
        data_frame = ttk.LabelFrame(parent, text="Data Preview & Results", padding="10")
        data_frame.grid(row=1, column=1, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Create notebook for tabs
        self.notebook = ttk.Notebook(data_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # Data preview tab
        self.preview_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.preview_frame, text="📊 Data Preview")
        
        # Create preview controls
        self.create_preview_controls()
        
        # Create treeview for data display
        self.create_data_treeview()
        
        # Quality report tab
        self.report_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.report_frame, text="📈 Quality Report")
        
        self.report_text = scrolledtext.ScrolledText(self.report_frame, wrap=tk.WORD)
        self.report_text.pack(fill=tk.BOTH, expand=True)
        
        # Log tab
        self.log_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.log_frame, text="📝 Processing Log")
        
        self.log_text = scrolledtext.ScrolledText(self.log_frame, wrap=tk.WORD)
        self.log_text.pack(fill=tk.BOTH, expand=True)
    
    def create_preview_controls(self):
        """Create preview navigation controls."""
        controls_frame = ttk.Frame(self.preview_frame)
        controls_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Preview info label
        self.preview_info_var = tk.StringVar(value="No data to preview")
        self.preview_info_label = ttk.Label(controls_frame, textvariable=self.preview_info_var, style='Header.TLabel')
        self.preview_info_label.pack(side=tk.LEFT)
        
        # Navigation buttons frame
        nav_frame = ttk.Frame(controls_frame)
        nav_frame.pack(side=tk.RIGHT)
        
        # Previous button
        self.prev_button = ttk.Button(nav_frame, text="◀ Previous", 
                                    command=self.prev_preview, state='disabled')
        self.prev_button.pack(side=tk.LEFT, padx=(0, 5))
        
        # Next button
        self.next_button = ttk.Button(nav_frame, text="Next ▶", 
                                    command=self.next_preview, state='disabled')
        self.next_button.pack(side=tk.LEFT)
        
        # Refresh button
        self.refresh_button = ttk.Button(nav_frame, text="🔄 Refresh", 
                                       command=self.refresh_preview, state='disabled')
        self.refresh_button.pack(side=tk.LEFT, padx=(5, 0))

    def create_data_treeview(self):
        """Create the data treeview widget."""
        # Frame for treeview and scrollbars
        tree_frame = ttk.Frame(self.preview_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        
        # Treeview
        self.tree = ttk.Treeview(tree_frame)
        
        # Scrollbars
        v_scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        h_scrollbar = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        
        self.tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        # Grid layout
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        v_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        h_scrollbar.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)
    
    def create_status_bar(self, parent):
        """Create the status bar."""
        self.status_var = tk.StringVar(value="Ready")
        status_label = ttk.Label(parent, textvariable=self.status_var, relief=tk.SUNKEN)
        status_label.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0))
    
    def select_files(self):
        """Select CSV files to process."""
        files = filedialog.askopenfilenames(
            title="Select CSV Files",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        
        if files:
            self.uploaded_files = list(files)
            self.update_file_list()
            self.status_var.set(f"Selected {len(files)} file(s)")
    
    def update_file_list(self):
        """Update the file listbox."""
        self.file_listbox.delete(0, tk.END)
        for file_path in self.uploaded_files:
            filename = os.path.basename(file_path)
            self.file_listbox.insert(tk.END, filename)
    
    def clear_files(self):
        """Clear selected files."""
        self.uploaded_files = []
        self.update_file_list()
        self.processed_data = {}
        self.quality_report = None
        self.clear_data_display()
        self.status_var.set("Files cleared")
    
    def clear_data_display(self):
        """Clear the data display."""
        # Clear treeview
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Clear report text
        self.report_text.delete(1.0, tk.END)
        self.log_text.delete(1.0, tk.END)
        
        # Reset preview controls
        self.preview_dataframes = {}
        self.current_preview_index = 0
        self.update_preview_controls()
    
    def process_files(self):
        """Process the uploaded files."""
        if not self.uploaded_files:
            messagebox.showwarning("No Files", "Please select CSV files first.")
            return
        
        # Run processing in a separate thread to prevent GUI freezing
        thread = threading.Thread(target=self._process_files_thread)
        thread.daemon = True
        thread.start()
    
    def _process_files_thread(self):
        """Process files in a separate thread."""
        try:
            self.status_var.set("Processing files...")
            self.root.update()
            
            # Read CSV files
            dataframes, encodings = read_multiple_csvs(
                self.uploaded_files,
                delimiter=self.delimiter_var.get(),
                decimal=self.decimal_var.get(),
                thousands=None
            )
            
            if not dataframes:
                self.root.after(0, lambda: messagebox.showerror("Error", "No files could be read successfully."))
                return
            
            # Clean dataframes
            cleaned_dataframes = {}
            processing_log = []
            
            for name, df in dataframes.items():
                try:
                    # Parse date and datetime columns
                    date_columns = None
                    if self.date_columns_var.get().strip():
                        date_columns = [col.strip() for col in self.date_columns_var.get().split(',') if col.strip()]
                    
                    datetime_columns = None
                    if self.datetime_columns_var.get().strip():
                        datetime_columns = [col.strip() for col in self.datetime_columns_var.get().split(',') if col.strip()]
                    
                    # Apply cleaning
                    cleaned_df = clean_dataframe(
                        df,
                        standardize_cols=self.standardize_cols_var.get(),
                        infer_dtypes_flag=self.infer_dtypes_var.get(),
                        trim_whitespace_flag=self.trim_whitespace_var.get(),
                        na_policy=self.na_policy_var.get(),
                        na_fill_value=self.fill_value_var.get() if self.na_policy_var.get() == 'fill' else None,
                        date_columns=date_columns,
                        datetime_columns=datetime_columns,
                        datetime_format=self.datetime_format_var.get(),
                        auto_detect_datetime=self.auto_detect_datetime_var.get(),
                        remove_duplicates_flag=self.remove_duplicates_var.get()
                    )
                    
                    cleaned_dataframes[name] = cleaned_df
                    
                    # Log processing
                    processing_log.append({
                        'name': f'Clean {name}',
                        'timestamp': pd.Timestamp.now().isoformat(),
                        'status': 'success',
                        'details': f'Processed {len(df)} rows, {len(df.columns)} columns'
                    })
                    
                except Exception as e:
                    processing_log.append({
                        'name': f'Clean {name}',
                        'timestamp': pd.Timestamp.now().isoformat(),
                        'status': 'error',
                        'errors': [str(e)]
                    })
            
            # Prepare for Excel export
            merge_mode = self.merge_mode_var.get()
            excel_data = prepare_excel_export(cleaned_dataframes, merge_mode)
            
            # Generate quality report
            quality_report = generate_quality_report(cleaned_dataframes)
            
            # Update GUI in main thread
            self.root.after(0, lambda: self._update_after_processing(excel_data, quality_report, processing_log))
            
        except Exception as e:
            self.root.after(0, lambda: messagebox.showerror("Processing Error", f"Error processing files: {str(e)}"))
            self.root.after(0, lambda: self.status_var.set("Error processing files"))
    
    def _update_after_processing(self, excel_data, quality_report, processing_log):
        """Update GUI after processing is complete."""
        self.processed_data = excel_data
        self.quality_report = quality_report
        self.processing_log = processing_log
        
        # Update data display
        self.update_data_display()
        
        # Update quality report
        self.update_quality_report()
        
        # Update processing log
        self.update_processing_log()
        
        self.status_var.set("Processing complete!")
        messagebox.showinfo("Success", "Files processed successfully!")
    
    def update_data_display(self):
        """Update the data display with processed data."""
        # Clear existing data
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        if not self.processed_data:
            self.preview_info_var.set("No data to preview")
            self.prev_button.config(state='disabled')
            self.next_button.config(state='disabled')
            self.refresh_button.config(state='disabled')
            return
        
        # Store dataframes for preview cycling
        self.preview_dataframes = self.processed_data.copy()
        self.current_preview_index = 0
        
        # Update preview controls
        self.update_preview_controls()
        
        # Display current preview
        self.show_current_preview()
    
    def update_preview_controls(self):
        """Update preview navigation controls."""
        if not self.preview_dataframes:
            self.preview_info_var.set("No data to preview")
            self.prev_button.config(state='disabled')
            self.next_button.config(state='disabled')
            self.refresh_button.config(state='disabled')
            return
        
        total_datasets = len(self.preview_dataframes)
        current_dataset = list(self.preview_dataframes.keys())[self.current_preview_index]
        
        self.preview_info_var.set(f"Dataset {self.current_preview_index + 1} of {total_datasets}: {current_dataset}")
        
        # Enable/disable navigation buttons
        self.prev_button.config(state='normal' if self.current_preview_index > 0 else 'disabled')
        self.next_button.config(state='normal' if self.current_preview_index < total_datasets - 1 else 'disabled')
        self.refresh_button.config(state='normal')
    
    def show_current_preview(self):
        """Show the current preview dataset."""
        if not self.preview_dataframes:
            return
        
        # Get current dataset
        current_key = list(self.preview_dataframes.keys())[self.current_preview_index]
        df = self.preview_dataframes[current_key]
        
        # Clear existing data
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Set up columns
        columns = list(df.columns)
        self.tree['columns'] = columns
        self.tree['show'] = 'headings'
        
        # Configure column headings
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=100, minwidth=50)
        
        # Insert data (limit to first 100 rows for performance)
        for i, row in df.head(100).iterrows():
            values = [str(val) if pd.notna(val) else "" for val in row]
            self.tree.insert('', 'end', values=values)
    
    def prev_preview(self):
        """Show previous dataset preview."""
        if self.current_preview_index > 0:
            self.current_preview_index -= 1
            self.update_preview_controls()
            self.show_current_preview()
    
    def next_preview(self):
        """Show next dataset preview."""
        if self.current_preview_index < len(self.preview_dataframes) - 1:
            self.current_preview_index += 1
            self.update_preview_controls()
            self.show_current_preview()
    
    def refresh_preview(self):
        """Refresh current preview."""
        self.show_current_preview()
    
    def update_quality_report(self):
        """Update the quality report display."""
        if not self.quality_report:
            return
        
        report_text = format_report_text(self.quality_report)
        self.report_text.delete(1.0, tk.END)
        self.report_text.insert(1.0, report_text)
    
    def update_processing_log(self):
        """Update the processing log display."""
        if not self.processing_log:
            return
        
        log_text = generate_processing_log(self.processing_log)
        self.log_text.delete(1.0, tk.END)
        self.log_text.insert(1.0, log_text)
    
    def download_excel(self):
        """Download processed data as Excel file."""
        if not self.processed_data:
            messagebox.showwarning("No Data", "Please process files first.")
            return
        
        file_path = filedialog.asksaveasfilename(
            title="Save Excel File",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )
        
        if file_path:
            try:
                success = safe_write_excel(self.processed_data, file_path)
                if success:
                    messagebox.showinfo("Success", f"Excel file saved to: {file_path}")
                    self.status_var.set(f"Excel file saved: {os.path.basename(file_path)}")
                else:
                    messagebox.showerror("Error", "Failed to save Excel file")
            except Exception as e:
                messagebox.showerror("Error", f"Error saving Excel file: {str(e)}")
    
    def download_report(self):
        """Download quality report as text file."""
        if not self.quality_report:
            messagebox.showwarning("No Report", "Please process files first.")
            return
        
        file_path = filedialog.asksaveasfilename(
            title="Save Quality Report",
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        
        if file_path:
            try:
                report_text = format_report_text(self.quality_report)
                if self.processing_log:
                    log_text = generate_processing_log(self.processing_log)
                    report_text += "\n\n" + log_text
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(report_text)
                
                messagebox.showinfo("Success", f"Report saved to: {file_path}")
                self.status_var.set(f"Report saved: {os.path.basename(file_path)}")
            except Exception as e:
                messagebox.showerror("Error", f"Error saving report: {str(e)}")
    


def main():
    """Main function to run the application."""
    root = tk.Tk()
    app = DataWranglerApp(root)
    
    # Center the window
    root.update_idletasks()
    x = (root.winfo_screenwidth() // 2) - (root.winfo_width() // 2)
    y = (root.winfo_screenheight() // 2) - (root.winfo_height() // 2)
    root.geometry(f"+{x}+{y}")
    
    root.mainloop()


if __name__ == "__main__":
    main()
