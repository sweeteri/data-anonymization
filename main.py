import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
from functions import all_quasi_identifiers, anonymize_dataset, calculate_k_anonymity


def select_file():
    global filename
    filename = filedialog.askopenfilename(title="Выберите файл",
                                          filetypes=(("CSV files", "*.csv"), ("All files", "*.*")))
    if filename:
        messagebox.showinfo("Выбор файла", "Файл выбран: " + filename)


def anonymize():
    if not filename:
        messagebox.showwarning("Ошибка", "Сначала выберите файл!")
        return

    selected_quasi = [q for q in all_quasi_identifiers if quasi_vars[q].get()]

    if not selected_quasi:
        messagebox.showwarning("Ошибка", "Выберите хотя бы один квази-идентификатор!")
        return

    anonymized_df = anonymize_dataset(filename, selected_quasi)
    anonymized_df.to_csv('anonymized_dataset.csv', index=False)
    messagebox.showinfo("Успех", "Датасет успешно обезличен и сохранен!")


def calculate_k_anonymity_func():
    global k_anonymity_results
    try:
        df = pd.read_csv(filename)
        df_cleaned, k_anonymity, top_5_bad_k_values, unique_lines, percentage_of_deleted_data = calculate_k_anonymity(
            df)
        df_cleaned.to_csv('anonymized_dataset_cleaned.csv', index=False)
        top_k_text.delete(1.0, tk.END)
        for index, row in top_5_bad_k_values.iterrows():
            top_k_text.insert(tk.END, f"K-анонимити: {row['k_value']}, Процент: {row['percentage_of_rows']:.2f}%\n")

        messagebox.showinfo("Результат",
                            f"K-анонимити: {k_anonymity}, Удалено: {percentage_of_deleted_data:.2f}% строк")

    except Exception as e:
        messagebox.showerror("Ошибка", f"Произошла ошибка при расчете K-анонимити: {str(e)}")


root = tk.Tk()
root.title("Обезличивание данных и K-анонимити")
root.geometry("600x400")

filename = ""
k_anonymity_results = None
quasi_vars = {}

button_frame = tk.Frame(root)
button_frame.pack(pady=10)

select_button = tk.Button(button_frame, text="Выбрать файл", command=select_file)
select_button.pack(side=tk.LEFT, padx=10)

anonymize_button = tk.Button(button_frame, text="Обезличить", command=anonymize)
anonymize_button.pack(side=tk.LEFT, padx=10)

calculate_button = tk.Button(button_frame, text="Рассчитать K-анонимити", command=calculate_k_anonymity_func)
calculate_button.pack(side=tk.LEFT, padx=10)

quasi_frame = tk.Frame(root)
quasi_frame.pack(side=tk.LEFT, padx=10, pady=10)

tk.Label(quasi_frame, text="Квази-идентификаторы").pack()

for quasi in all_quasi_identifiers:
    var = tk.BooleanVar()
    chk = tk.Checkbutton(quasi_frame, text=quasi, variable=var)
    chk.pack(anchor='w')
    quasi_vars[quasi] = var

result_frame = tk.Frame(root)
result_frame.pack(side=tk.RIGHT, padx=10, pady=10)

tk.Label(result_frame, text="Топ 5 плохих K-анонимити").pack()

top_k_text = tk.Text(result_frame, width=40, height=15)
top_k_text.pack()

root.mainloop()
