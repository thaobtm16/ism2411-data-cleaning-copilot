
## What Copilot generated
Copilot mostly helped by suggesting common data-cleaning code. When I wrote comments like “load the CSV” or started typing pd.read_csv, it completed full blocks for reading the file, checking .info(), dropping missing values, and converting columns with pd.to_numeric(errors='coerce'). It also suggested simple functions to clean column names using lowercase and replacing spaces.

## What you modified
I changed several parts of Copilot’s suggestions to make the code fit my dataset. I renamed variables to more meaningful names, changed some cleaning steps so they wouldn’t remove too much data, and replaced slow loops with faster pandas methods. For example, instead of the loop Copilot generated to clean text, I used a vectorized method like df['city'].str.strip().str.title(). These changes made the code clearer, more efficient, and more accurate.

## What you learned
I learned that data cleaning in Python requires careful decision-making. Tools like df.info() helped me understand data types before choosing how to clean them. I also learned that Copilot is helpful for generating quick code, but it doesn’t understand my dataset, so I still need to edit and double-check everything. One example was Copilot suggesting df.dropna() for the whole dataset, but I changed it to target only certain columns so I wouldn’t lose important rows.