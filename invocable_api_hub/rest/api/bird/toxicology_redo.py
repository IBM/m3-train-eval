

from fastapi import APIRouter, Query
import sqlite3

app = APIRouter()
def get_db_connection():
    return sqlite3.connect('invocable_api_hub/db/bird/test/toxicology.sqlite', check_same_thread=False)

# Database connection
conn = sqlite3.connect('invocable_api_hub/db/bird/test/toxicology.sqlite', check_same_thread=False)
cursor = conn.cursor()

# Endpoint to get the most common bond type
@app.get("/v1/bird/toxicology/most_common_bond_type", summary="Get the most common bond type")
async def get_most_common_bond_type():
    query = """
    SELECT T.bond_type FROM (
        SELECT bond_type, COUNT(bond_id)
        FROM bond
        GROUP BY bond_type
        ORDER BY COUNT(bond_id) DESC
        LIMIT 1
    ) AS T
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"most_common_bond_type": result[0]}

# Endpoint to get count of distinct molecule IDs with specific element and label
@app.get("/v1/bird/toxicology/count_distinct_molecules", summary="Get count of distinct molecule IDs with specific element and label")
async def get_count_distinct_molecules(element: str = Query(..., description="Element to filter by"), label: str = Query(..., description="Label to filter by")):
    query = f"""
    SELECT COUNT(DISTINCT T1.molecule_id)
    FROM molecule AS T1
    INNER JOIN atom AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T2.element = '{element}' AND T1.label = '{label}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count_distinct_molecules": result[0]}

# Endpoint to get average oxygen count
@app.get("/v1/bird/toxicology/average_oxygen_count", summary="Get average oxygen count")
async def get_average_oxygen_count(bond_type: str = Query(..., description="Bond type to filter by")):
    query = f"""
    SELECT AVG(oxygen_count)
    FROM (
        SELECT T1.molecule_id, COUNT(T1.element) AS oxygen_count
        FROM atom AS T1
        INNER JOIN bond AS T2 ON T1.molecule_id = T2.molecule_id
        WHERE T2.bond_type = '{bond_type}' AND T1.element = 'o'
        GROUP BY T1.molecule_id
    ) AS oxygen_counts
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"average_oxygen_count": result[0]}

# Endpoint to get average single bond count
@app.get("/v1/bird/toxicology/average_single_bond_count", summary="Get average single bond count")
async def get_average_single_bond_count(bond_type: str = Query(..., description="Bond type to filter by"), label: str = Query(..., description="Label to filter by")):
    query = f"""
    SELECT AVG(single_bond_count)
    FROM (
        SELECT T3.molecule_id, COUNT(T1.bond_type) AS single_bond_count
        FROM bond AS T1
        INNER JOIN atom AS T2 ON T1.molecule_id = T2.molecule_id
        INNER JOIN molecule AS T3 ON T3.molecule_id = T2.molecule_id
        WHERE T1.bond_type = '{bond_type}' AND T3.label = '{label}'
        GROUP BY T3.molecule_id
    ) AS subquery
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"average_single_bond_count": result[0]}

# Endpoint to get count of distinct molecule IDs with specific element and label
@app.get("/v1/bird/toxicology/count_distinct_molecules_with_element", summary="Get count of distinct molecule IDs with specific element and label")
async def get_count_distinct_molecules_with_element(element: str = Query(..., description="Element to filter by"), label: str = Query(..., description="Label to filter by")):
    query = f"""
    SELECT COUNT(DISTINCT T2.molecule_id)
    FROM atom AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T1.element = '{element}' AND T2.label = '{label}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count_distinct_molecules_with_element": result[0]}

# Endpoint to get distinct molecule IDs with specific bond type and label
@app.get("/v1/bird/toxicology/distinct_molecule_ids", summary="Get distinct molecule IDs with specific bond type and label")
async def get_distinct_molecule_ids(bond_type: str = Query(..., description="Bond type to filter by"), label: str = Query(..., description="Label to filter by")):
    query = f"""
    SELECT DISTINCT T2.molecule_id
    FROM bond AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T1.bond_type = '{bond_type}' AND T2.label = '{label}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"distinct_molecule_ids": [row[0] for row in result]}

# Endpoint to get percentage of carbon atoms with specific bond type
@app.get("/v1/bird/toxicology/percentage_carbon_atoms", summary="Get percentage of carbon atoms with specific bond type")
async def get_percentage_carbon_atoms(bond_type: str = Query(..., description="Bond type to filter by")):
    query = f"""
    SELECT CAST(COUNT(DISTINCT CASE WHEN T1.element = 'c' THEN T1.atom_id ELSE NULL END) AS REAL) * 100 / COUNT(DISTINCT T1.atom_id)
    FROM atom AS T1
    INNER JOIN bond AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T2.bond_type = '{bond_type}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"percentage_carbon_atoms": result[0]}

# Endpoint to get count of bonds with specific bond type
@app.get("/v1/bird/toxicology/count_bonds", summary="Get count of bonds with specific bond type")
async def get_count_bonds(bond_type: str = Query(..., description="Bond type to filter by")):
    query = f"""
    SELECT COUNT(T.bond_id)
    FROM bond AS T
    WHERE T.bond_type = '{bond_type}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count_bonds": result[0]}

# Endpoint to get count of distinct atoms with specific element
@app.get("/v1/bird/toxicology/count_distinct_atoms", summary="Get count of distinct atoms with specific element")
async def get_count_distinct_atoms(element: str = Query(..., description="Element to filter by")):
    query = f"""
    SELECT COUNT(DISTINCT T.atom_id)
    FROM atom AS T
    WHERE T.element <> '{element}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count_distinct_atoms": result[0]}

# Endpoint to get count of molecules within a specific range and label
@app.get("/v1/bird/toxicology/count_molecules_in_range", summary="Get count of molecules within a specific range and label")
async def get_count_molecules_in_range(start_id: str = Query(..., description="Start molecule ID"), end_id: str = Query(..., description="End molecule ID"), label: str = Query(..., description="Label to filter by")):
    query = f"""
    SELECT COUNT(T.molecule_id)
    FROM molecule AS T
    WHERE T.molecule_id BETWEEN '{start_id}' AND '{end_id}' AND T.label = '{label}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"count_molecules_in_range": result[0]}

# Endpoint to get molecule IDs with specific element
@app.get("/v1/bird/toxicology/molecule_ids_with_element", summary="Get molecule IDs with specific element")
async def get_molecule_ids_with_element(element: str = Query(..., description="Element to filter by")):
    query = f"""
    SELECT T.molecule_id
    FROM atom AS T
    WHERE T.element = '{element}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"molecule_ids_with_element": [row[0] for row in result]}

# Endpoint to get distinct elements with specific bond ID
@app.get("/v1/bird/toxicology/distinct_elements_with_bond_id", summary="Get distinct elements with specific bond ID")
async def get_distinct_elements_with_bond_id(bond_id: str = Query(..., description="Bond ID to filter by")):
    query = f"""
    SELECT DISTINCT T1.element
    FROM atom AS T1
    INNER JOIN connected AS T2 ON T1.atom_id = T2.atom_id
    WHERE T2.bond_id = '{bond_id}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"distinct_elements_with_bond_id": [row[0] for row in result]}

# Endpoint to get distinct elements with specific bond type
@app.get("/v1/bird/toxicology/distinct_elements_with_bond_type", summary="Get distinct elements with specific bond type")
async def get_distinct_elements_with_bond_type(bond_type: str = Query(..., description="Bond type to filter by")):
    query = f"""
    SELECT DISTINCT T1.element
    FROM atom AS T1
    INNER JOIN bond AS T2 ON T1.molecule_id = T2.molecule_id
    INNER JOIN connected AS T3 ON T1.atom_id = T3.atom_id
    WHERE T2.bond_type = '{bond_type}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"distinct_elements_with_bond_type": [row[0] for row in result]}

# Endpoint to get the most common label for hydrogen atoms
@app.get("/v1/bird/toxicology/most_common_label_for_hydrogen", summary="Get the most common label for hydrogen atoms")
async def get_most_common_label_for_hydrogen():
    query = """
    SELECT T.label
    FROM (
        SELECT T2.label, COUNT(T2.molecule_id)
        FROM atom AS T1
        INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
        WHERE T1.element = 'h'
        GROUP BY T2.label
        ORDER BY COUNT(T2.molecule_id) DESC
        LIMIT 1
    ) t
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"most_common_label_for_hydrogen": result[0]}

# Endpoint to get distinct bond types with specific element
@app.get("/v1/bird/toxicology/distinct_bond_types_with_element", summary="Get distinct bond types with specific element")
async def get_distinct_bond_types_with_element(element: str = Query(..., description="Element to filter by")):
    query = f"""
    SELECT DISTINCT T1.bond_type
    FROM bond AS T1
    INNER JOIN connected AS T2 ON T1.bond_id = T2.bond_id
    INNER JOIN atom AS T3 ON T2.atom_id = T3.atom_id
    WHERE T3.element = '{element}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"distinct_bond_types_with_element": [row[0] for row in result]}

# Endpoint to get atom IDs with specific bond type
@app.get("/v1/bird/toxicology/atom_ids_with_bond_type", summary="Get atom IDs with specific bond type")
async def get_atom_ids_with_bond_type(bond_type: str = Query(..., description="Bond type to filter by")):
    query = f"""
    SELECT T2.atom_id, T2.atom_id2
    FROM bond AS T1
    INNER JOIN connected AS T2 ON T1.bond_id = T2.bond_id
    WHERE T1.bond_type = '{bond_type}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"atom_ids_with_bond_type": [{"atom_id": row[0], "atom_id2": row[1]} for row in result]}

# Endpoint to get distinct atom IDs with specific label
@app.get("/v1/bird/toxicology/distinct_atom_ids_with_label", summary="Get distinct atom IDs with specific label")
async def get_distinct_atom_ids_with_label(label: str = Query(..., description="Label to filter by")):
    query = f"""
    SELECT DISTINCT T1.atom_id
    FROM atom AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    INNER JOIN connected AS T3 ON T1.atom_id = T3.atom_id
    WHERE T2.label = '{label}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"distinct_atom_ids_with_label": [row[0] for row in result]}

# Endpoint to get the least common element with specific label
@app.get("/v1/bird/toxicology/least_common_element_with_label", summary="Get the least common element with specific label")
async def get_least_common_element_with_label(label: str = Query(..., description="Label to filter by")):
    query = f"""
    SELECT T.element
    FROM (
        SELECT T1.element, COUNT(DISTINCT T1.molecule_id)
        FROM atom AS T1
        INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
        WHERE T2.label = '{label}'
        GROUP BY T1.element
        ORDER BY COUNT(DISTINCT T1.molecule_id) ASC
        LIMIT 1
    ) t
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"least_common_element_with_label": result[0]}

# Endpoint to get bond types with specific atom IDs
@app.get("/v1/bird/toxicology/bond_types_with_atom_ids", summary="Get bond types with specific atom IDs")
async def get_bond_types_with_atom_ids(atom_id1: str = Query(..., description="First atom ID to filter by"), atom_id2: str = Query(..., description="Second atom ID to filter by")):
    query = f"""
    SELECT T1.bond_type
    FROM bond AS T1
    INNER JOIN connected AS T2 ON T1.bond_id = T2.bond_id
    WHERE (T2.atom_id = '{atom_id1}' AND T2.atom_id2 = '{atom_id2}') OR (T2.atom_id2 = '{atom_id1}' AND T2.atom_id = '{atom_id2}')
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"bond_types_with_atom_ids": [row[0] for row in result]}

# Endpoint to get distinct labels with specific element
@app.get("/v1/bird/toxicology/distinct_labels_with_element", summary="Get distinct labels with specific element")
async def get_distinct_labels_with_element(element: str = Query(..., description="Element to filter by")):
    query = f"""
    SELECT DISTINCT T2.label
    FROM atom AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T1.element != '{element}'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return {"distinct_labels_with_element": [row[0] for row in result]}



# Endpoint to get counts of iodine and sulfur atoms
@app.get("/v1/bird/toxicology/atom_counts", summary="Get counts of iodine and sulfur atoms")
async def get_atom_counts(bond_type: str = Query(..., description="Type of bond")):
    query = """
    SELECT COUNT(DISTINCT CASE WHEN T1.element = 'i' THEN T1.atom_id ELSE NULL END) AS iodine_nums,
           COUNT(DISTINCT CASE WHEN T1.element = 's' THEN T1.atom_id ELSE NULL END) AS sulfur_nums
    FROM atom AS T1
    INNER JOIN connected AS T2 ON T1.atom_id = T2.atom_id
    INNER JOIN bond AS T3 ON T2.bond_id = T3.bond_id
    WHERE T3.bond_type = ?
    """
    cursor.execute(query, (bond_type,))
    result = cursor.fetchall()
    return result

# Endpoint to get atom IDs for a specific bond type
@app.get("/v1/bird/toxicology/bond_atoms", summary="Get atom IDs for a specific bond type")
async def get_bond_atoms(bond_type: str = Query(..., description="Type of bond")):
    query = """
    SELECT T2.atom_id, T2.atom_id2
    FROM bond AS T1
    INNER JOIN connected AS T2 ON T1.bond_id = T2.bond_id
    WHERE T1.bond_type = ?
    """
    cursor.execute(query, (bond_type,))
    result = cursor.fetchall()
    return result

# Endpoint to get atom IDs for a specific molecule
@app.get("/v1/bird/toxicology/molecule_atoms", summary="Get atom IDs for a specific molecule")
async def get_molecule_atoms(molecule_id: str = Query(..., description="ID of the molecule")):
    query = """
    SELECT T2.atom_id, T2.atom_id2
    FROM atom AS T1
    INNER JOIN connected AS T2 ON T2.atom_id = T1.atom_id
    WHERE T1.molecule_id = ?
    """
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of molecules without fluorine
@app.get("/v1/bird/toxicology/molecule_percentage", summary="Get percentage of molecules without fluorine")
async def get_molecule_percentage(label: str = Query(..., description="Label of the molecule")):
    query = """
    SELECT CAST(COUNT(DISTINCT CASE WHEN T1.element <> 'f' THEN T2.molecule_id ELSE NULL END) AS REAL) * 100 / COUNT(DISTINCT T2.molecule_id)
    FROM atom AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T2.label = ?
    """
    cursor.execute(query, (label,))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of molecules with a specific bond type and label
@app.get("/v1/bird/toxicology/molecule_bond_percentage", summary="Get percentage of molecules with a specific bond type and label")
async def get_molecule_bond_percentage(bond_type: str = Query(..., description="Type of bond"), label: str = Query(..., description="Label of the molecule")):
    query = """
    SELECT CAST(COUNT(DISTINCT CASE WHEN T2.label = ? THEN T2.molecule_id ELSE NULL END) AS REAL) * 100 / COUNT(DISTINCT T2.molecule_id)
    FROM atom AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    INNER JOIN bond AS T3 ON T2.molecule_id = T3.molecule_id
    WHERE T3.bond_type = ?
    """
    cursor.execute(query, (label, bond_type))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct elements for a specific molecule
@app.get("/v1/bird/toxicology/molecule_elements", summary="Get distinct elements for a specific molecule")
async def get_molecule_elements(molecule_id: str = Query(..., description="ID of the molecule")):
    query = """
    SELECT DISTINCT T.element
    FROM atom AS T
    WHERE T.molecule_id = ?
    ORDER BY T.element
    LIMIT 3
    """
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get atom IDs for a specific bond
@app.get("/v1/bird/toxicology/bond_atom_ids", summary="Get atom IDs for a specific bond")
async def get_bond_atom_ids(molecule_id: str = Query(..., description="ID of the molecule"), bond_id: str = Query(..., description="ID of the bond")):
    query = """
    SELECT SUBSTR(T.bond_id, 1, 7) AS atom_id1, T.molecule_id || SUBSTR(T.bond_id, 8, 2) AS atom_id2
    FROM bond AS T
    WHERE T.molecule_id = ? AND T.bond_id = ?
    """
    cursor.execute(query, (molecule_id, bond_id))
    result = cursor.fetchall()
    return result

# Endpoint to get difference between positive and negative labeled molecules
@app.get("/v1/bird/toxicology/molecule_label_diff", summary="Get difference between positive and negative labeled molecules")
async def get_molecule_label_diff():
    query = """
    SELECT COUNT(CASE WHEN T.label = '+' THEN T.molecule_id ELSE NULL END) - COUNT(CASE WHEN T.label = '-' THEN T.molecule_id ELSE NULL END) AS diff_car_notcar
    FROM molecule t
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get atom IDs for a specific bond
@app.get("/v1/bird/toxicology/connected_atom_ids", summary="Get atom IDs for a specific bond")
async def get_connected_atom_ids(bond_id: str = Query(..., description="ID of the bond")):
    query = """
    SELECT T.atom_id
    FROM connected AS T
    WHERE T.bond_id = ?
    """
    cursor.execute(query, (bond_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get bond IDs for a specific atom
@app.get("/v1/bird/toxicology/connected_bond_ids", summary="Get bond IDs for a specific atom")
async def get_connected_bond_ids(atom_id2: str = Query(..., description="ID of the atom")):
    query = """
    SELECT T.bond_id
    FROM connected AS T
    WHERE T.atom_id2 = ?
    """
    cursor.execute(query, (atom_id2,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct molecule IDs for a specific bond type
@app.get("/v1/bird/toxicology/bond_molecule_ids", summary="Get distinct molecule IDs for a specific bond type")
async def get_bond_molecule_ids(bond_type: str = Query(..., description="Type of bond")):
    query = """
    SELECT DISTINCT T.molecule_id
    FROM bond AS T
    WHERE T.bond_type = ?
    ORDER BY T.molecule_id
    LIMIT 5
    """
    cursor.execute(query, (bond_type,))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of bonds of a specific type in a molecule
@app.get("/v1/bird/toxicology/bond_percentage", summary="Get percentage of bonds of a specific type in a molecule")
async def get_bond_percentage(molecule_id: str = Query(..., description="ID of the molecule")):
    query = """
    SELECT ROUND(CAST(COUNT(CASE WHEN T.bond_type = '=' THEN T.bond_id ELSE NULL END) AS REAL) * 100 / COUNT(T.bond_id), 5)
    FROM bond AS T
    WHERE T.molecule_id = ?
    """
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of positively labeled molecules
@app.get("/v1/bird/toxicology/positive_molecule_percentage", summary="Get percentage of positively labeled molecules")
async def get_positive_molecule_percentage():
    query = """
    SELECT ROUND(CAST(COUNT(CASE WHEN T.label = '+' THEN T.molecule_id ELSE NULL END) AS REAL) * 100 / COUNT(T.molecule_id), 3)
    FROM molecule t
    """
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of hydrogen atoms in a molecule
@app.get("/v1/bird/toxicology/hydrogen_atom_percentage", summary="Get percentage of hydrogen atoms in a molecule")
async def get_hydrogen_atom_percentage(molecule_id: str = Query(..., description="ID of the molecule")):
    query = """
    SELECT ROUND(CAST(COUNT(CASE WHEN T.element = 'h' THEN T.atom_id ELSE NULL END) AS REAL) * 100 / COUNT(T.atom_id), 4)
    FROM atom AS T
    WHERE T.molecule_id = ?
    """
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct bond types for a specific molecule
@app.get("/v1/bird/toxicology/molecule_bond_types", summary="Get distinct bond types for a specific molecule")
async def get_molecule_bond_types(molecule_id: str = Query(..., description="ID of the molecule")):
    query = """
    SELECT DISTINCT T.bond_type
    FROM bond AS T
    WHERE T.molecule_id = ?
    """
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct elements and labels for a specific molecule
@app.get("/v1/bird/toxicology/molecule_elements_labels", summary="Get distinct elements and labels for a specific molecule")
async def get_molecule_elements_labels(molecule_id: str = Query(..., description="ID of the molecule")):
    query = """
    SELECT DISTINCT T1.element, T2.label
    FROM atom AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T2.molecule_id = ?
    """
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get the most common bond type for a specific molecule
@app.get("/v1/bird/toxicology/most_common_bond_type", summary="Get the most common bond type for a specific molecule")
async def get_most_common_bond_type(molecule_id: str = Query(..., description="ID of the molecule")):
    query = """
    SELECT T.bond_type
    FROM (
        SELECT T1.bond_type, COUNT(T1.molecule_id)
        FROM bond AS T1
        WHERE T1.molecule_id = ?
        GROUP BY T1.bond_type
        ORDER BY COUNT(T1.molecule_id) DESC
        LIMIT 1
    ) AS T
    """
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct molecule IDs for a specific bond type and label
@app.get("/v1/bird/toxicology/bond_label_molecule_ids", summary="Get distinct molecule IDs for a specific bond type and label")
async def get_bond_label_molecule_ids(bond_type: str = Query(..., description="Type of bond"), label: str = Query(..., description="Label of the molecule")):
    query = """
    SELECT DISTINCT T2.molecule_id
    FROM bond AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T1.bond_type = ? AND T2.label = ?
    ORDER BY T2.molecule_id
    LIMIT 3
    """
    cursor.execute(query, (bond_type, label))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct bond IDs for a specific molecule
@app.get("/v1/bird/toxicology/molecule_bond_ids", summary="Get distinct bond IDs for a specific molecule")
async def get_molecule_bond_ids(molecule_id: str = Query(..., description="ID of the molecule")):
    query = """
    SELECT DISTINCT T2.bond_id
    FROM atom AS T1
    INNER JOIN connected AS T2 ON T1.atom_id = T2.atom_id
    WHERE T1.molecule_id = ?
    ORDER BY T2.bond_id
    LIMIT 2
    """
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of bonds for a specific molecule and atom IDs
@app.get("/v1/bird/toxicology/bond_count", summary="Get count of bonds for a specific molecule and atom IDs")
async def get_bond_count(molecule_id: str = Query(..., description="ID of the molecule"), atom_id: str = Query(..., description="ID of the atom"), atom_id2: str = Query(..., description="ID of the second atom")):
    query = """
    SELECT COUNT(T2.bond_id)
    FROM bond AS T1
    INNER JOIN connected AS T2 ON T1.bond_id = T2.bond_id
    WHERE T1.molecule_id = ? AND T2.atom_id = ? AND T2.atom_id2 = ?
    """
    cursor.execute(query, (molecule_id, atom_id, atom_id2))
    result = cursor.fetchall()
    return result

# Endpoint to get count of distinct molecule_id
@app.get("/v1/bird/toxicology/count_distinct_molecule_id", summary="Get count of distinct molecule_id based on label and element")
async def get_count_distinct_molecule_id(label: str = Query(..., description="Label of the molecule"), element: str = Query(..., description="Element of the atom")):
    query = f"SELECT COUNT(DISTINCT T2.molecule_id) FROM atom AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T2.label = ? AND T1.element = ?"
    cursor.execute(query, (label, element))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get bond details
@app.get("/v1/bird/toxicology/bond_details", summary="Get bond details based on bond_id")
async def get_bond_details(bond_id: str = Query(..., description="ID of the bond")):
    query = f"SELECT T1.bond_type, T2.atom_id, T2.atom_id2 FROM bond AS T1 INNER JOIN connected AS T2 ON T1.bond_id = T2.bond_id WHERE T2.bond_id = ?"
    cursor.execute(query, (bond_id,))
    result = cursor.fetchall()
    return {"bond_details": result}

# Endpoint to get carcinogenic flag
@app.get("/v1/bird/toxicology/carcinogenic_flag", summary="Get carcinogenic flag based on atom_id")
async def get_carcinogenic_flag(atom_id: str = Query(..., description="ID of the atom")):
    query = f"SELECT T2.molecule_id, IIF(T2.label = '+', 'YES', 'NO') AS flag_carcinogenic FROM atom AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T1.atom_id = ?"
    cursor.execute(query, (atom_id,))
    result = cursor.fetchall()
    return {"carcinogenic_flag": result}

# Endpoint to get count of distinct molecule_id based on bond_type
@app.get("/v1/bird/toxicology/count_distinct_molecule_id_by_bond_type", summary="Get count of distinct molecule_id based on bond_type")
async def get_count_distinct_molecule_id_by_bond_type(bond_type: str = Query(..., description="Type of the bond")):
    query = f"SELECT COUNT(DISTINCT T.molecule_id) FROM bond AS T WHERE T.bond_type = ?"
    cursor.execute(query, (bond_type,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of bond_id based on atom_id suffix
@app.get("/v1/bird/toxicology/count_bond_id_by_atom_id_suffix", summary="Get count of bond_id based on atom_id suffix")
async def get_count_bond_id_by_atom_id_suffix(atom_id_suffix: str = Query(..., description="Suffix of the atom_id")):
    query = f"SELECT COUNT(T.bond_id) FROM connected AS T WHERE SUBSTR(T.atom_id, -2) = ?"
    cursor.execute(query, (atom_id_suffix,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get distinct elements based on molecule_id
@app.get("/v1/bird/toxicology/distinct_elements_by_molecule_id", summary="Get distinct elements based on molecule_id")
async def get_distinct_elements_by_molecule_id(molecule_id: str = Query(..., description="ID of the molecule")):
    query = f"SELECT DISTINCT T.element FROM atom AS T WHERE T.molecule_id = ?"
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return {"elements": result}

# Endpoint to get count of molecule_id based on label
@app.get("/v1/bird/toxicology/count_molecule_id_by_label", summary="Get count of molecule_id based on label")
async def get_count_molecule_id_by_label(label: str = Query(..., description="Label of the molecule")):
    query = f"SELECT COUNT(T.molecule_id) FROM molecule AS T WHERE T.label = ?"
    cursor.execute(query, (label,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get distinct molecule_id based on atom_id range and label
@app.get("/v1/bird/toxicology/distinct_molecule_id_by_atom_id_range_and_label", summary="Get distinct molecule_id based on atom_id range and label")
async def get_distinct_molecule_id_by_atom_id_range_and_label(atom_id_start: str = Query(..., description="Start of the atom_id range"), atom_id_end: str = Query(..., description="End of the atom_id range"), label: str = Query(..., description="Label of the molecule")):
    query = f"SELECT DISTINCT T2.molecule_id FROM atom AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE SUBSTR(T1.atom_id, -2) BETWEEN ? AND ? AND T2.label = ?"
    cursor.execute(query, (atom_id_start, atom_id_end, label))
    result = cursor.fetchall()
    return {"molecule_ids": result}

# Endpoint to get bond_id based on element conditions
@app.get("/v1/bird/toxicology/bond_id_by_element_conditions", summary="Get bond_id based on element conditions")
async def get_bond_id_by_element_conditions(element1: str = Query(..., description="First element"), element2: str = Query(..., description="Second element")):
    query = f"""
    SELECT T2.bond_id FROM atom AS T1
    INNER JOIN connected AS T2 ON T1.atom_id = T2.atom_id
    WHERE T2.bond_id IN (
        SELECT T3.bond_id FROM connected AS T3
        INNER JOIN atom AS T4 ON T3.atom_id = T4.atom_id
        WHERE T4.element = ?
    ) AND T1.element = ?
    """
    cursor.execute(query, (element2, element1))
    result = cursor.fetchall()
    return {"bond_ids": result}

# Endpoint to get label based on bond_type count
@app.get("/v1/bird/toxicology/label_by_bond_type_count", summary="Get label based on bond_type count")
async def get_label_by_bond_type_count(bond_type: str = Query(..., description="Type of the bond")):
    query = f"""
    SELECT T1.label FROM molecule AS T1
    INNER JOIN (
        SELECT T.molecule_id, COUNT(T.bond_type)
        FROM bond AS T
        WHERE T.bond_type = ?
        GROUP BY T.molecule_id
        ORDER BY COUNT(T.bond_type) DESC
        LIMIT 1
    ) AS T2 ON T1.molecule_id = T2.molecule_id
    """
    cursor.execute(query, (bond_type,))
    result = cursor.fetchall()
    return {"labels": result}

# Endpoint to get bond to atom ratio based on element
@app.get("/v1/bird/toxicology/bond_to_atom_ratio_by_element", summary="Get bond to atom ratio based on element")
async def get_bond_to_atom_ratio_by_element(element: str = Query(..., description="Element of the atom")):
    query = f"SELECT CAST(COUNT(T2.bond_id) AS REAL) / COUNT(T1.atom_id) FROM atom AS T1 INNER JOIN connected AS T2 ON T1.atom_id = T2.atom_id WHERE T1.element = ?"
    cursor.execute(query, (element,))
    result = cursor.fetchone()
    return {"ratio": result[0]}

# Endpoint to get bond details based on atom_id substring
@app.get("/v1/bird/toxicology/bond_details_by_atom_id_substring", summary="Get bond details based on atom_id substring")
async def get_bond_details_by_atom_id_substring(atom_id_substring: str = Query(..., description="Substring of the atom_id")):
    query = f"SELECT T1.bond_type, T1.bond_id FROM bond AS T1 INNER JOIN connected AS T2 ON T1.bond_id = T2.bond_id WHERE SUBSTR(T2.atom_id, 7, 2) = ?"
    cursor.execute(query, (atom_id_substring,))
    result = cursor.fetchall()
    return {"bond_details": result}

# Endpoint to get distinct elements not in connected atoms
@app.get("/v1/bird/toxicology/distinct_elements_not_in_connected_atoms", summary="Get distinct elements not in connected atoms")
async def get_distinct_elements_not_in_connected_atoms():
    query = f"SELECT DISTINCT T.element FROM atom AS T WHERE T.element NOT IN (SELECT DISTINCT T1.element FROM atom AS T1 INNER JOIN connected AS T2 ON T1.atom_id = T2.atom_id)"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"elements": result}

# Endpoint to get atom details based on bond_type and molecule_id
@app.get("/v1/bird/toxicology/atom_details_by_bond_type_and_molecule_id", summary="Get atom details based on bond_type and molecule_id")
async def get_atom_details_by_bond_type_and_molecule_id(bond_type: str = Query(..., description="Type of the bond"), molecule_id: str = Query(..., description="ID of the molecule")):
    query = f"SELECT T2.atom_id, T2.atom_id2 FROM atom AS T1 INNER JOIN connected AS T2 ON T1.atom_id = T2.atom_id INNER JOIN bond AS T3 ON T2.bond_id = T3.bond_id WHERE T3.bond_type = ? AND T3.molecule_id = ?"
    cursor.execute(query, (bond_type, molecule_id))
    result = cursor.fetchall()
    return {"atom_details": result}

# Endpoint to get element based on bond_id
@app.get("/v1/bird/toxicology/element_by_bond_id", summary="Get element based on bond_id")
async def get_element_by_bond_id(bond_id: str = Query(..., description="ID of the bond")):
    query = f"SELECT T2.element FROM connected AS T1 INNER JOIN atom AS T2 ON T1.atom_id = T2.atom_id WHERE T1.bond_id = ?"
    cursor.execute(query, (bond_id,))
    result = cursor.fetchall()
    return {"elements": result}

# Endpoint to get molecule_id based on bond_type and label
@app.get("/v1/bird/toxicology/molecule_id_by_bond_type_and_label", summary="Get molecule_id based on bond_type and label")
async def get_molecule_id_by_bond_type_and_label(bond_type: str = Query(..., description="Type of the bond"), label: str = Query(..., description="Label of the molecule")):
    query = f"""
    SELECT T.molecule_id FROM (
        SELECT T3.molecule_id, COUNT(T1.bond_type)
        FROM bond AS T1
        INNER JOIN molecule AS T3 ON T1.molecule_id = T3.molecule_id
        WHERE T3.label = ? AND T1.bond_type = ?
        GROUP BY T3.molecule_id
        ORDER BY COUNT(T1.bond_type) DESC
        LIMIT 1
    ) AS T
    """
    cursor.execute(query, (label, bond_type))
    result = cursor.fetchall()
    return {"molecule_ids": result}

# Endpoint to get element based on label
@app.get("/v1/bird/toxicology/element_by_label", summary="Get element based on label")
async def get_element_by_label(label: str = Query(..., description="Label of the molecule")):
    query = f"""
    SELECT T.element FROM (
        SELECT T2.element, COUNT(DISTINCT T2.molecule_id)
        FROM molecule AS T1
        INNER JOIN atom AS T2 ON T1.molecule_id = T2.molecule_id
        WHERE T1.label = ?
        GROUP BY T2.element
        ORDER BY COUNT(DISTINCT T2.molecule_id)
        LIMIT 1
    ) t
    """
    cursor.execute(query, (label,))
    result = cursor.fetchall()
    return {"elements": result}

# Endpoint to get atom details based on element
@app.get("/v1/bird/toxicology/atom_details_by_element", summary="Get atom details based on element")
async def get_atom_details_by_element(element: str = Query(..., description="Element of the atom")):
    query = f"SELECT T2.atom_id, T2.atom_id2 FROM atom AS T1 INNER JOIN connected AS T2 ON T1.atom_id = T2.atom_id WHERE T1.element = ?"
    cursor.execute(query, (element,))
    result = cursor.fetchall()
    return {"atom_details": result}

# Endpoint to get distinct elements based on bond_type
@app.get("/v1/bird/toxicology/distinct_elements_by_bond_type", summary="Get distinct elements based on bond_type")
async def get_distinct_elements_by_bond_type(bond_type: str = Query(..., description="Type of the bond")):
    query = f"SELECT DISTINCT T3.element FROM bond AS T1 INNER JOIN connected AS T2 ON T1.bond_id = T2.bond_id INNER JOIN atom AS T3 ON T2.atom_id = T3.atom_id WHERE T1.bond_type = ?"
    cursor.execute(query, (bond_type,))
    result = cursor.fetchall()
    return {"elements": result}

# Endpoint to get percentage of most common bond_type
@app.get("/v1/bird/toxicology/percentage_most_common_bond_type", summary="Get percentage of most common bond_type")
async def get_percentage_most_common_bond_type():
    query = f"""
    SELECT CAST((
        SELECT COUNT(T1.atom_id)
        FROM connected AS T1
        INNER JOIN bond AS T2 ON T1.bond_id = T2.bond_id
        GROUP BY T2.bond_type
        ORDER BY COUNT(T2.bond_id) DESC
        LIMIT 1
    ) AS REAL) * 100 / (
        SELECT COUNT(atom_id)
        FROM connected
    )
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get bond percentage
@app.get("/v1/bird/toxicology/bond_percentage", summary="Get bond percentage for a given bond type")
async def get_bond_percentage(bond_type: str = Query(..., description="Type of the bond")):
    query = f"""
    SELECT ROUND(CAST(COUNT(CASE WHEN T2.label = '+' THEN T1.bond_id ELSE NULL END) AS REAL) * 100 / COUNT(T1.bond_id),5)
    FROM bond AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T1.bond_type = ?
    """
    cursor.execute(query, (bond_type,))
    result = cursor.fetchone()
    return {"bond_percentage": result[0]}

# Endpoint to get atom count
@app.get("/v1/bird/toxicology/atom_count", summary="Get count of atoms for given elements")
async def get_atom_count(elements: str = Query(..., description="Comma-separated list of elements")):
    elements_list = elements.split(',')
    placeholders = ','.join(['?'] * len(elements_list))
    query = f"""
    SELECT COUNT(T.atom_id)
    FROM atom AS T
    WHERE T.element IN ({placeholders})
    """
    cursor.execute(query, elements_list)
    result = cursor.fetchone()
    return {"atom_count": result[0]}

# Endpoint to get distinct atom ids
@app.get("/v1/bird/toxicology/distinct_atom_ids", summary="Get distinct atom ids for a given element")
async def get_distinct_atom_ids(element: str = Query(..., description="Element symbol")):
    query = """
    SELECT DISTINCT T2.atom_id2
    FROM atom AS T1
    INNER JOIN connected AS T2 ON T1.atom_id = T2.atom_id
    WHERE T1.element = ?
    """
    cursor.execute(query, (element,))
    result = cursor.fetchall()
    return {"distinct_atom_ids": [row[0] for row in result]}

# Endpoint to get distinct bond types
@app.get("/v1/bird/toxicology/distinct_bond_types", summary="Get distinct bond types for a given element")
async def get_distinct_bond_types(element: str = Query(..., description="Element symbol")):
    query = """
    SELECT DISTINCT T3.bond_type
    FROM atom AS T1
    INNER JOIN connected AS T2 ON T1.atom_id = T2.atom_id
    INNER JOIN bond AS T3 ON T3.bond_id = T2.bond_id
    WHERE T1.element = ?
    """
    cursor.execute(query, (element,))
    result = cursor.fetchall()
    return {"distinct_bond_types": [row[0] for row in result]}

# Endpoint to get count of distinct elements
@app.get("/v1/bird/toxicology/distinct_element_count", summary="Get count of distinct elements for a given bond type")
async def get_distinct_element_count(bond_type: str = Query(..., description="Type of the bond")):
    query = """
    SELECT COUNT(DISTINCT T.element)
    FROM (
        SELECT DISTINCT T2.molecule_id, T1.element
        FROM atom AS T1
        INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
        INNER JOIN bond AS T3 ON T2.molecule_id = T3.molecule_id
        WHERE T3.bond_type = ?
    ) AS T
    """
    cursor.execute(query, (bond_type,))
    result = cursor.fetchone()
    return {"distinct_element_count": result[0]}

# Endpoint to get atom count for specific elements and bond type
@app.get("/v1/bird/toxicology/atom_count_for_elements_and_bond_type", summary="Get atom count for specific elements and bond type")
async def get_atom_count_for_elements_and_bond_type(bond_type: str = Query(..., description="Type of the bond"), elements: str = Query(..., description="Comma-separated list of elements")):
    elements_list = elements.split(',')
    placeholders = ','.join(['?'] * len(elements_list))
    query = f"""
    SELECT COUNT(T1.atom_id)
    FROM atom AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    INNER JOIN bond AS T3 ON T2.molecule_id = T3.molecule_id
    WHERE T3.bond_type = ? AND T1.element IN ({placeholders})
    """
    cursor.execute(query, [bond_type] + elements_list)
    result = cursor.fetchone()
    return {"atom_count": result[0]}

# Endpoint to get distinct bond ids for a given label
@app.get("/v1/bird/toxicology/distinct_bond_ids", summary="Get distinct bond ids for a given label")
async def get_distinct_bond_ids(label: str = Query(..., description="Label of the molecule")):
    query = """
    SELECT DISTINCT T1.bond_id
    FROM bond AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T2.label = ?
    """
    cursor.execute(query, (label,))
    result = cursor.fetchall()
    return {"distinct_bond_ids": [row[0] for row in result]}

# Endpoint to get distinct molecule ids for a given label and bond type
@app.get("/v1/bird/toxicology/distinct_molecule_ids", summary="Get distinct molecule ids for a given label and bond type")
async def get_distinct_molecule_ids(label: str = Query(..., description="Label of the molecule"), bond_type: str = Query(..., description="Type of the bond")):
    query = """
    SELECT DISTINCT T1.molecule_id
    FROM bond AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T2.label = ? AND T1.bond_type = ?
    """
    cursor.execute(query, (label, bond_type))
    result = cursor.fetchall()
    return {"distinct_molecule_ids": [row[0] for row in result]}

# Endpoint to get percentage of atoms for a given element and bond type
@app.get("/v1/bird/toxicology/atom_percentage", summary="Get percentage of atoms for a given element and bond type")
async def get_atom_percentage(element: str = Query(..., description="Element symbol"), bond_type: str = Query(..., description="Type of the bond")):
    query = """
    SELECT CAST(COUNT(CASE WHEN T.element = ? THEN T.atom_id ELSE NULL END) AS REAL) * 100 / COUNT(T.atom_id)
    FROM (
        SELECT T1.atom_id, T1.element
        FROM atom AS T1
        INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
        INNER JOIN bond AS T3 ON T2.molecule_id = T3.molecule_id
        WHERE T3.bond_type = ?
    ) AS T
    """
    cursor.execute(query, (element, bond_type))
    result = cursor.fetchone()
    return {"atom_percentage": result[0]}

# Endpoint to get molecule details for given molecule ids
@app.get("/v1/bird/toxicology/molecule_details", summary="Get molecule details for given molecule ids")
async def get_molecule_details(molecule_ids: str = Query(..., description="Comma-separated list of molecule ids")):
    molecule_ids_list = molecule_ids.split(',')
    placeholders = ','.join(['?'] * len(molecule_ids_list))
    query = f"""
    SELECT molecule_id, T.label
    FROM molecule AS T
    WHERE T.molecule_id IN ({placeholders})
    """
    cursor.execute(query, molecule_ids_list)
    result = cursor.fetchall()
    return {"molecule_details": [{"molecule_id": row[0], "label": row[1]} for row in result]}

# Endpoint to get molecule ids for a given label
@app.get("/v1/bird/toxicology/molecule_ids_for_label", summary="Get molecule ids for a given label")
async def get_molecule_ids_for_label(label: str = Query(..., description="Label of the molecule")):
    query = """
    SELECT T.molecule_id
    FROM molecule AS T
    WHERE T.label = ?
    """
    cursor.execute(query, (label,))
    result = cursor.fetchall()
    return {"molecule_ids": [row[0] for row in result]}

# Endpoint to get count of molecule ids for a given range and label
@app.get("/v1/bird/toxicology/molecule_count_for_range_and_label", summary="Get count of molecule ids for a given range and label")
async def get_molecule_count_for_range_and_label(start_id: str = Query(..., description="Start molecule id"), end_id: str = Query(..., description="End molecule id"), label: str = Query(..., description="Label of the molecule")):
    query = """
    SELECT COUNT(T.molecule_id)
    FROM molecule AS T
    WHERE T.molecule_id BETWEEN ? AND ? AND T.label = ?
    """
    cursor.execute(query, (start_id, end_id, label))
    result = cursor.fetchone()
    return {"molecule_count": result[0]}

# Endpoint to get bond details for a given range
@app.get("/v1/bird/toxicology/bond_details_for_range", summary="Get bond details for a given range")
async def get_bond_details_for_range(start_id: str = Query(..., description="Start molecule id"), end_id: str = Query(..., description="End molecule id")):
    query = """
    SELECT T2.molecule_id, T2.bond_type
    FROM molecule AS T1
    INNER JOIN bond AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T1.molecule_id BETWEEN ? AND ?
    """
    cursor.execute(query, (start_id, end_id))
    result = cursor.fetchall()
    return {"bond_details": [{"molecule_id": row[0], "bond_type": row[1]} for row in result]}

# Endpoint to get element details for a given bond id
@app.get("/v1/bird/toxicology/element_details_for_bond_id", summary="Get element details for a given bond id")
async def get_element_details_for_bond_id(bond_id: str = Query(..., description="Bond id")):
    query = """
    SELECT T2.element
    FROM connected AS T1
    INNER JOIN atom AS T2 ON T1.atom_id = T2.atom_id
    WHERE T1.bond_id = ?
    """
    cursor.execute(query, (bond_id,))
    result = cursor.fetchall()
    return {"element_details": [row[0] for row in result]}

# Endpoint to get bond count for a given element
@app.get("/v1/bird/toxicology/bond_count_for_element", summary="Get bond count for a given element")
async def get_bond_count_for_element(element: str = Query(..., description="Element symbol")):
    query = """
    SELECT COUNT(T3.bond_id)
    FROM atom AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    INNER JOIN bond AS T3 ON T2.molecule_id = T3.molecule_id
    WHERE T1.element = ?
    """
    cursor.execute(query, (element,))
    result = cursor.fetchone()
    return {"bond_count": result[0]}

# Endpoint to get label for a given element
@app.get("/v1/bird/toxicology/label_for_element", summary="Get label for a given element")
async def get_label_for_element(element: str = Query(..., description="Element symbol")):
    query = """
    SELECT T2.label
    FROM atom AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T1.element = ?
    GROUP BY T2.label
    ORDER BY COUNT(T2.label) DESC
    LIMIT 1
    """
    cursor.execute(query, (element,))
    result = cursor.fetchone()
    return {"label": result[0]}

# Endpoint to get bond details for a given bond id and elements
@app.get("/v1/bird/toxicology/bond_details_for_bond_id_and_elements", summary="Get bond details for a given bond id and elements")
async def get_bond_details_for_bond_id_and_elements(bond_id: str = Query(..., description="Bond id"), elements: str = Query(..., description="Comma-separated list of elements")):
    elements_list = elements.split(',')
    placeholders = ','.join(['?'] * len(elements_list))
    query = f"""
    SELECT T2.bond_id, T2.atom_id2, T1.element AS flag_have_CaCl
    FROM atom AS T1
    INNER JOIN connected AS T2 ON T2.atom_id = T1.atom_id
    WHERE T2.bond_id = ? AND T1.element IN ({placeholders})
    """
    cursor.execute(query, [bond_id] + elements_list)
    result = cursor.fetchall()
    return {"bond_details": [{"bond_id": row[0], "atom_id2": row[1], "flag_have_CaCl": row[2]} for row in result]}

# Endpoint to get distinct molecule ids for a given bond type and element
@app.get("/v1/bird/toxicology/distinct_molecule_ids_for_bond_type_and_element", summary="Get distinct molecule ids for a given bond type and element")
async def get_distinct_molecule_ids_for_bond_type_and_element(bond_type: str = Query(..., description="Type of the bond"), element: str = Query(..., description="Element symbol"), label: str = Query(..., description="Label of the molecule")):
    query = """
    SELECT DISTINCT T2.molecule_id
    FROM atom AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    INNER JOIN bond AS T3 ON T2.molecule_id = T3.molecule_id
    WHERE T3.bond_type = ? AND T1.element = ? AND T2.label = ?
    """
    cursor.execute(query, (bond_type, element, label))
    result = cursor.fetchall()
    return {"distinct_molecule_ids": [row[0] for row in result]}

# Endpoint to get percentage of atoms for a given element and label
@app.get("/v1/bird/toxicology/atom_percentage_for_element_and_label", summary="Get percentage of atoms for a given element and label")
async def get_atom_percentage_for_element_and_label(element: str = Query(..., description="Element symbol"), label: str = Query(..., description="Label of the molecule")):
    query = """
    SELECT CAST(COUNT(CASE WHEN T1.element = ? THEN T1.element ELSE NULL END) AS REAL) * 100 / COUNT(T1.element)
    FROM atom AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T2.label = ?
    """
    cursor.execute(query, (element, label))
    result = cursor.fetchone()
    return {"atom_percentage": result[0]}

# Endpoint to get distinct elements for a given molecule id
@app.get("/v1/bird/toxicology/distinct_elements_for_molecule_id", summary="Get distinct elements for a given molecule id")
async def get_distinct_elements_for_molecule_id(molecule_id: str = Query(..., description="Molecule id")):
    query = """
    SELECT DISTINCT T.element
    FROM atom AS T
    WHERE T.molecule_id = ?
    """
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return {"distinct_elements": [row[0] for row in result]}

# Endpoint to get distinct molecule_id based on bond_type
@app.get("/v1/bird/toxicology/distinct_molecule_ids", summary="Get distinct molecule IDs based on bond type")
async def get_distinct_molecule_ids(bond_type: str = Query(..., description="Type of bond")):
    query = f"SELECT DISTINCT T.molecule_id FROM bond AS T WHERE T.bond_type = ?"
    cursor.execute(query, (bond_type,))
    result = cursor.fetchall()
    return {"molecule_ids": result}

# Endpoint to get atom_id and atom_id2 based on bond_type
@app.get("/v1/bird/toxicology/connected_atoms", summary="Get connected atoms based on bond type")
async def get_connected_atoms(bond_type: str = Query(..., description="Type of bond")):
    query = f"SELECT T2.atom_id, T2.atom_id2 FROM bond AS T1 INNER JOIN connected AS T2 ON T1.bond_id = T2.bond_id WHERE T1.bond_type = ?"
    cursor.execute(query, (bond_type,))
    result = cursor.fetchall()
    return {"connected_atoms": result}

# Endpoint to get element based on bond_id
@app.get("/v1/bird/toxicology/element_by_bond_id", summary="Get element based on bond ID")
async def get_element_by_bond_id(bond_id: str = Query(..., description="ID of the bond")):
    query = f"SELECT T1.element FROM atom AS T1 INNER JOIN connected AS T2 ON T1.atom_id = T2.atom_id WHERE T2.bond_id = ?"
    cursor.execute(query, (bond_id,))
    result = cursor.fetchall()
    return {"elements": result}

# Endpoint to get count of distinct molecule_ids based on label and bond_type
@app.get("/v1/bird/toxicology/count_distinct_molecule_ids", summary="Get count of distinct molecule IDs based on label and bond type")
async def get_count_distinct_molecule_ids(label: str = Query(..., description="Label of the molecule"), bond_type: str = Query(..., description="Type of bond")):
    query = f"SELECT COUNT(DISTINCT T2.molecule_id) FROM bond AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T2.label = ? AND T1.bond_type = ?"
    cursor.execute(query, (label, bond_type))
    result = cursor.fetchall()
    return {"count": result}

# Endpoint to get label based on bond_id
@app.get("/v1/bird/toxicology/label_by_bond_id", summary="Get label based on bond ID")
async def get_label_by_bond_id(bond_id: str = Query(..., description="ID of the bond")):
    query = f"SELECT T2.label FROM bond AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T1.bond_id = ?"
    cursor.execute(query, (bond_id,))
    result = cursor.fetchall()
    return {"labels": result}

# Endpoint to get distinct bond_id and label based on bond_type
@app.get("/v1/bird/toxicology/distinct_bond_ids_and_labels", summary="Get distinct bond IDs and labels based on bond type")
async def get_distinct_bond_ids_and_labels(bond_type: str = Query(..., description="Type of bond")):
    query = f"SELECT DISTINCT T1.bond_id, T2.label FROM bond AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T1.bond_type = ?"
    cursor.execute(query, (bond_type,))
    result = cursor.fetchall()
    return {"bond_ids_and_labels": result}

# Endpoint to get distinct element based on label, atom_id suffix, and atom_id length
@app.get("/v1/bird/toxicology/distinct_elements", summary="Get distinct elements based on label, atom ID suffix, and atom ID length")
async def get_distinct_elements(label: str = Query(..., description="Label of the molecule"), atom_id_suffix: str = Query(..., description="Suffix of the atom ID"), atom_id_length: int = Query(..., description="Length of the atom ID")):
    query = f"SELECT DISTINCT T1.element FROM atom AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T2.label = ? AND SUBSTR(T1.atom_id, -1) = ? AND LENGTH(T1.atom_id) = ?"
    cursor.execute(query, (label, atom_id_suffix, atom_id_length))
    result = cursor.fetchall()
    return {"elements": result}

# Endpoint to get ratio of hydrogen atoms based on molecule_id
@app.get("/v1/bird/toxicology/hydrogen_ratio", summary="Get ratio of hydrogen atoms based on molecule ID")
async def get_hydrogen_ratio(molecule_id: str = Query(..., description="ID of the molecule")):
    query = f"""
    WITH SubQuery AS (
        SELECT DISTINCT T1.atom_id, T1.element, T1.molecule_id, T2.label
        FROM atom AS T1
        INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
        WHERE T2.molecule_id = ?
    )
    SELECT CAST(COUNT(CASE WHEN element = 'h' THEN atom_id ELSE NULL END) AS REAL) /
           (CASE WHEN COUNT(atom_id) = 0 THEN NULL ELSE COUNT(atom_id) END) AS ratio, label
    FROM SubQuery
    GROUP BY label
    """
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return {"hydrogen_ratio": result}

# Endpoint to get flag_carcinogenic based on element
@app.get("/v1/bird/toxicology/flag_carcinogenic_by_element", summary="Get flag carcinogenic based on element")
async def get_flag_carcinogenic_by_element(element: str = Query(..., description="Element")):
    query = f"SELECT T2.label AS flag_carcinogenic FROM atom AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T1.element = ?"
    cursor.execute(query, (element,))
    result = cursor.fetchall()
    return {"flag_carcinogenic": result}

# Endpoint to get distinct bond_type based on element
@app.get("/v1/bird/toxicology/distinct_bond_types", summary="Get distinct bond types based on element")
async def get_distinct_bond_types(element: str = Query(..., description="Element")):
    query = f"SELECT DISTINCT T2.bond_type FROM atom AS T1 INNER JOIN bond AS T2 ON T1.molecule_id = T2.molecule_id WHERE T1.element = ?"
    cursor.execute(query, (element,))
    result = cursor.fetchall()
    return {"bond_types": result}

# Endpoint to get element based on bond_id
@app.get("/v1/bird/toxicology/element_by_bond_id_v2", summary="Get element based on bond ID")
async def get_element_by_bond_id_v2(bond_id: str = Query(..., description="ID of the bond")):
    query = f"SELECT T1.element FROM atom AS T1 INNER JOIN connected AS T2 ON T1.atom_id = T2.atom_id INNER JOIN bond AS T3 ON T2.bond_id = T3.bond_id WHERE T3.bond_id = ?"
    cursor.execute(query, (bond_id,))
    result = cursor.fetchall()
    return {"elements": result}

# Endpoint to get percentage of bonds of a specific type
@app.get("/v1/bird/toxicology/bond_type_percentage", summary="Get percentage of bonds of a specific type")
async def get_bond_type_percentage(bond_type: str = Query(..., description="Type of bond")):
    query = f"SELECT CAST(COUNT(CASE WHEN T.bond_type = ? THEN T.bond_id ELSE NULL END) AS REAL) * 100 / COUNT(T.bond_id) FROM bond AS T"
    cursor.execute(query, (bond_type,))
    result = cursor.fetchall()
    return {"percentage": result}

# Endpoint to get percentage of bonds of a specific type for a specific molecule
@app.get("/v1/bird/toxicology/bond_type_percentage_by_molecule", summary="Get percentage of bonds of a specific type for a specific molecule")
async def get_bond_type_percentage_by_molecule(bond_type: str = Query(..., description="Type of bond"), molecule_id: str = Query(..., description="ID of the molecule")):
    query = f"SELECT CAST(COUNT(CASE WHEN T.bond_type = ? THEN T.bond_id ELSE NULL END) AS REAL) * 100 / COUNT(T.bond_id) FROM bond AS T WHERE T.molecule_id = ?"
    cursor.execute(query, (bond_type, molecule_id))
    result = cursor.fetchall()
    return {"percentage": result}

# Endpoint to get flag_carcinogenic based on atom_id
@app.get("/v1/bird/toxicology/flag_carcinogenic_by_atom_id", summary="Get flag carcinogenic based on atom ID")
async def get_flag_carcinogenic_by_atom_id(atom_id: str = Query(..., description="ID of the atom")):
    query = f"SELECT T2.label AS flag_carcinogenic FROM atom AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T1.atom_id = ?"
    cursor.execute(query, (atom_id,))
    result = cursor.fetchall()
    return {"flag_carcinogenic": result}

# Endpoint to get label based on molecule_id
@app.get("/v1/bird/toxicology/label_by_molecule_id", summary="Get label based on molecule ID")
async def get_label_by_molecule_id(molecule_id: str = Query(..., description="ID of the molecule")):
    query = f"SELECT T.label FROM molecule AS T WHERE T.molecule_id = ?"
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return {"labels": result}

# Endpoint to get distinct element based on molecule_id
@app.get("/v1/bird/toxicology/distinct_elements_by_molecule_id", summary="Get distinct elements based on molecule ID")
async def get_distinct_elements_by_molecule_id(molecule_id: str = Query(..., description="ID of the molecule")):
    query = f"SELECT DISTINCT T.element FROM atom AS T WHERE T.molecule_id = ?"
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return {"elements": result}

# Endpoint to get count of molecule_ids based on label
@app.get("/v1/bird/toxicology/count_molecule_ids_by_label", summary="Get count of molecule IDs based on label")
async def get_count_molecule_ids_by_label(label: str = Query(..., description="Label of the molecule")):
    query = f"SELECT COUNT(T.molecule_id) FROM molecule AS T WHERE T.label = ?"
    cursor.execute(query, (label,))
    result = cursor.fetchall()
    return {"count": result}

# Endpoint to get atom_id based on molecule_id range and element
@app.get("/v1/bird/toxicology/atom_ids_by_molecule_id_range_and_element", summary="Get atom IDs based on molecule ID range and element")
async def get_atom_ids_by_molecule_id_range_and_element(start_molecule_id: str = Query(..., description="Start molecule ID"), end_molecule_id: str = Query(..., description="End molecule ID"), element: str = Query(..., description="Element")):
    query = f"SELECT T.atom_id FROM atom AS T WHERE T.molecule_id BETWEEN ? AND ? AND T.element = ?"
    cursor.execute(query, (start_molecule_id, end_molecule_id, element))
    result = cursor.fetchall()
    return {"atom_ids": result}

# Endpoint to get count of atom_ids based on label
@app.get("/v1/bird/toxicology/count_atom_ids_by_label", summary="Get count of atom IDs based on label")
async def get_count_atom_ids_by_label(label: str = Query(..., description="Label of the molecule")):
    query = f"SELECT COUNT(T1.atom_id) FROM atom AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T2.label = ?"
    cursor.execute(query, (label,))
    result = cursor.fetchall()
    return {"count": result}

# Endpoint to get bond_id based on label and bond_type
@app.get("/v1/bird/toxicology/bond_ids_by_label_and_bond_type", summary="Get bond IDs based on label and bond type")
async def get_bond_ids_by_label_and_bond_type(label: str = Query(..., description="Label of the molecule"), bond_type: str = Query(..., description="Type of bond")):
    query = f"SELECT T1.bond_id FROM bond AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T2.label = ? AND T1.bond_type = ?"
    cursor.execute(query, (label, bond_type))
    result = cursor.fetchall()
    return {"bond_ids": result}

# Endpoint to get the count of hydrogen atoms in molecules with a specific label
@app.get("/v1/bird/toxicology/atomnums_h", summary="Get count of hydrogen atoms in molecules with a specific label")
async def get_atomnums_h(label: str = Query(..., description="Label of the molecule"), element: str = Query(..., description="Element of the atom")):
    query = f"SELECT COUNT(T1.atom_id) AS atomnums_h FROM atom AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T2.label = ? AND T1.element = ?"
    cursor.execute(query, (label, element))
    result = cursor.fetchall()
    return result

# Endpoint to get connected atoms and bonds for a specific atom and bond ID
@app.get("/v1/bird/toxicology/connected_atoms_bonds", summary="Get connected atoms and bonds for a specific atom and bond ID")
async def get_connected_atoms_bonds(atom_id: str = Query(..., description="ID of the atom"), bond_id: str = Query(..., description="ID of the bond")):
    query = f"SELECT T2.molecule_id, T2.bond_id, T1.atom_id FROM connected AS T1 INNER JOIN bond AS T2 ON T1.bond_id = T2.bond_id WHERE T1.atom_id = ? AND T2.bond_id = ?"
    cursor.execute(query, (atom_id, bond_id))
    result = cursor.fetchall()
    return result

# Endpoint to get atom IDs for a specific element and molecule label
@app.get("/v1/bird/toxicology/atom_ids", summary="Get atom IDs for a specific element and molecule label")
async def get_atom_ids(element: str = Query(..., description="Element of the atom"), label: str = Query(..., description="Label of the molecule")):
    query = f"SELECT T1.atom_id FROM atom AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T1.element = ? AND T2.label = ?"
    cursor.execute(query, (element, label))
    result = cursor.fetchall()
    return result

# Endpoint to get the percentage of hydrogen atoms in molecules with a specific label
@app.get("/v1/bird/toxicology/hydrogen_percentage", summary="Get the percentage of hydrogen atoms in molecules with a specific label")
async def get_hydrogen_percentage(element: str = Query(..., description="Element of the atom"), label: str = Query(..., description="Label of the molecule")):
    query = f"SELECT CAST(COUNT(CASE WHEN T1.element = ? AND T2.label = ? THEN T2.molecule_id ELSE NULL END) AS REAL) * 100 / COUNT(T2.molecule_id) FROM atom AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id"
    cursor.execute(query, (element, label))
    result = cursor.fetchall()
    return result

# Endpoint to get the label of a specific molecule
@app.get("/v1/bird/toxicology/molecule_label", summary="Get the label of a specific molecule")
async def get_molecule_label(molecule_id: str = Query(..., description="ID of the molecule")):
    query = f"SELECT T.label FROM molecule AS T WHERE T.molecule_id = ?"
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get atom IDs for a specific molecule
@app.get("/v1/bird/toxicology/molecule_atoms", summary="Get atom IDs for a specific molecule")
async def get_molecule_atoms(molecule_id: str = Query(..., description="ID of the molecule")):
    query = f"SELECT T.atom_id FROM atom AS T WHERE T.molecule_id = ?"
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get the bond type of a specific bond
@app.get("/v1/bird/toxicology/bond_type", summary="Get the bond type of a specific bond")
async def get_bond_type(bond_id: str = Query(..., description="ID of the bond")):
    query = f"SELECT T.bond_type FROM bond AS T WHERE T.bond_id = ?"
    cursor.execute(query, (bond_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct elements connected by a specific bond
@app.get("/v1/bird/toxicology/connected_elements", summary="Get distinct elements connected by a specific bond")
async def get_connected_elements(bond_id: str = Query(..., description="ID of the bond")):
    query = f"SELECT DISTINCT T1.element FROM atom AS T1 INNER JOIN connected AS T2 ON T1.atom_id = T2.atom_id WHERE T2.bond_id = ?"
    cursor.execute(query, (bond_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get the count of bonds and molecule labels for a specific bond type and molecule ID
@app.get("/v1/bird/toxicology/bond_count_by_label", summary="Get the count of bonds and molecule labels for a specific bond type and molecule ID")
async def get_bond_count_by_label(bond_type: str = Query(..., description="Type of the bond"), molecule_id: str = Query(..., description="ID of the molecule")):
    query = f"SELECT COUNT(T1.bond_id), T2.label FROM bond AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T1.bond_type = ? AND T2.molecule_id = ? GROUP BY T2.label"
    cursor.execute(query, (bond_type, molecule_id))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct molecule IDs and elements for a specific molecule label
@app.get("/v1/bird/toxicology/molecule_elements", summary="Get distinct molecule IDs and elements for a specific molecule label")
async def get_molecule_elements(label: str = Query(..., description="Label of the molecule")):
    query = f"SELECT DISTINCT T2.molecule_id, T1.element FROM atom AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T2.label = ?"
    cursor.execute(query, (label,))
    result = cursor.fetchall()
    return result

# Endpoint to get bond IDs and connected atom IDs for a specific bond type
@app.get("/v1/bird/toxicology/bond_connected_atoms", summary="Get bond IDs and connected atom IDs for a specific bond type")
async def get_bond_connected_atoms(bond_type: str = Query(..., description="Type of the bond")):
    query = f"SELECT T1.bond_id, T2.atom_id, T2.atom_id2 FROM bond AS T1 INNER JOIN connected AS T2 ON T1.bond_id = T2.bond_id WHERE T1.bond_type = ?"
    cursor.execute(query, (bond_type,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct molecule IDs and elements for a specific bond type
@app.get("/v1/bird/toxicology/bond_molecule_elements", summary="Get distinct molecule IDs and elements for a specific bond type")
async def get_bond_molecule_elements(bond_type: str = Query(..., description="Type of the bond")):
    query = f"SELECT DISTINCT T1.molecule_id, T2.element FROM bond AS T1 INNER JOIN atom AS T2 ON T1.molecule_id = T2.molecule_id WHERE T1.bond_type = ?"
    cursor.execute(query, (bond_type,))
    result = cursor.fetchall()
    return result

# Endpoint to get elements connected by a specific bond
@app.get("/v1/bird/toxicology/bond_elements", summary="Get elements connected by a specific bond")
async def get_bond_elements(bond_id: str = Query(..., description="ID of the bond")):
    query = f"SELECT T2.element FROM connected AS T1 INNER JOIN atom AS T2 ON T1.atom_id = T2.atom_id WHERE T1.bond_id = ?"
    cursor.execute(query, (bond_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get the count of bonds connected to atoms of a specific element
@app.get("/v1/bird/toxicology/bond_count_by_element", summary="Get the count of bonds connected to atoms of a specific element")
async def get_bond_count_by_element(element: str = Query(..., description="Element of the atom")):
    query = f"SELECT COUNT(T1.bond_id) FROM connected AS T1 INNER JOIN atom AS T2 ON T1.atom_id = T2.atom_id WHERE T2.element = ?"
    cursor.execute(query, (element,))
    result = cursor.fetchall()
    return result

# Endpoint to get atom IDs, bond types, and molecule IDs for a specific molecule ID
@app.get("/v1/bird/toxicology/atom_bond_types", summary="Get atom IDs, bond types, and molecule IDs for a specific molecule ID")
async def get_atom_bond_types(molecule_id: str = Query(..., description="ID of the molecule")):
    query = f"SELECT T1.atom_id, COUNT(DISTINCT T2.bond_type), T1.molecule_id FROM atom AS T1 INNER JOIN bond AS T2 ON T1.molecule_id = T2.molecule_id WHERE T1.molecule_id = ? GROUP BY T1.atom_id, T2.bond_type"
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get the count of distinct molecule IDs and the sum of molecules with a specific label for a specific bond type
@app.get("/v1/bird/toxicology/molecule_count_by_bond_type", summary="Get the count of distinct molecule IDs and the sum of molecules with a specific label for a specific bond type")
async def get_molecule_count_by_bond_type(bond_type: str = Query(..., description="Type of the bond"), label: str = Query(..., description="Label of the molecule")):
    query = f"SELECT COUNT(DISTINCT T2.molecule_id), SUM(CASE WHEN T2.label = ? THEN 1 ELSE 0 END) FROM bond AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T1.bond_type = ?"
    cursor.execute(query, (label, bond_type))
    result = cursor.fetchall()
    return result

# Endpoint to get the count of distinct molecule IDs for specific atom elements and bond types
@app.get("/v1/bird/toxicology/molecule_count_by_element_bond_type", summary="Get the count of distinct molecule IDs for specific atom elements and bond types")
async def get_molecule_count_by_element_bond_type(element: str = Query(..., description="Element of the atom"), bond_type: str = Query(..., description="Type of the bond")):
    query = f"SELECT COUNT(DISTINCT T1.molecule_id) FROM atom AS T1 INNER JOIN bond AS T2 ON T1.molecule_id = T2.molecule_id WHERE T1.element <> ? AND T2.bond_type <> ?"
    cursor.execute(query, (element, bond_type))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct molecule labels for a specific bond ID
@app.get("/v1/bird/toxicology/molecule_labels_by_bond", summary="Get distinct molecule labels for a specific bond ID")
async def get_molecule_labels_by_bond(bond_id: str = Query(..., description="ID of the bond")):
    query = f"SELECT DISTINCT T2.label FROM atom AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id INNER JOIN bond AS T3 ON T2.molecule_id = T3.molecule_id WHERE T3.bond_id = ?"
    cursor.execute(query, (bond_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get the count of atom IDs for a specific molecule ID
@app.get("/v1/bird/toxicology/atom_count_by_molecule", summary="Get the count of atom IDs for a specific molecule ID")
async def get_atom_count_by_molecule(molecule_id: str = Query(..., description="ID of the molecule")):
    query = f"SELECT COUNT(T.atom_id) FROM atom AS T WHERE T.molecule_id = ?"
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get the count of bond IDs for a specific bond type
@app.get("/v1/bird/toxicology/bond_count_by_type", summary="Get the count of bond IDs for a specific bond type")
async def get_bond_count_by_type(bond_type: str = Query(..., description="Type of the bond")):
    query = f"SELECT COUNT(T.bond_id) FROM bond AS T WHERE T.bond_type = ?"
    cursor.execute(query, (bond_type,))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct molecule_id based on element and label
@app.get("/v1/bird/toxicology/molecule_id_by_element_label", summary="Get distinct molecule_id based on element and label")
async def get_molecule_id_by_element_label(element: str = Query(..., description="Element type"), label: str = Query(..., description="Label type")):
    query = f"SELECT DISTINCT T1.molecule_id FROM atom AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T1.element = ? AND T2.label = ?"
    cursor.execute(query, (element, label))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of molecules with specific element and label
@app.get("/v1/bird/toxicology/percentage_molecules_by_element_label", summary="Get percentage of molecules with specific element and label")
async def get_percentage_molecules_by_element_label(element: str = Query(..., description="Element type"), label: str = Query(..., description="Label type")):
    query = f"SELECT COUNT(CASE WHEN T2.label = ? AND T1.element = ? THEN T2.molecule_id ELSE NULL END) * 100 / COUNT(T2.molecule_id) FROM atom AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id"
    cursor.execute(query, (label, element))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct molecule_id based on bond_id
@app.get("/v1/bird/toxicology/molecule_id_by_bond_id", summary="Get distinct molecule_id based on bond_id")
async def get_molecule_id_by_bond_id(bond_id: str = Query(..., description="Bond ID")):
    query = f"SELECT DISTINCT T1.molecule_id FROM atom AS T1 INNER JOIN connected AS T2 ON T1.atom_id = T2.atom_id WHERE T2.bond_id = ?"
    cursor.execute(query, (bond_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of distinct elements based on bond_id
@app.get("/v1/bird/toxicology/count_distinct_elements_by_bond_id", summary="Get count of distinct elements based on bond_id")
async def get_count_distinct_elements_by_bond_id(bond_id: str = Query(..., description="Bond ID")):
    query = f"SELECT COUNT(DISTINCT T1.element) FROM atom AS T1 INNER JOIN connected AS T2 ON T1.atom_id = T2.atom_id WHERE T2.bond_id = ?"
    cursor.execute(query, (bond_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get bond_type based on atom_id and atom_id2
@app.get("/v1/bird/toxicology/bond_type_by_atom_ids", summary="Get bond_type based on atom_id and atom_id2")
async def get_bond_type_by_atom_ids(atom_id: str = Query(..., description="Atom ID"), atom_id2: str = Query(..., description="Atom ID 2")):
    query = f"SELECT T1.bond_type FROM bond AS T1 INNER JOIN connected AS T2 ON T1.bond_id = T2.bond_id WHERE T2.atom_id = ? AND T2.atom_id2 = ?"
    cursor.execute(query, (atom_id, atom_id2))
    result = cursor.fetchall()
    return result

# Endpoint to get molecule_id based on atom_id and atom_id2
@app.get("/v1/bird/toxicology/molecule_id_by_atom_ids", summary="Get molecule_id based on atom_id and atom_id2")
async def get_molecule_id_by_atom_ids(atom_id: str = Query(..., description="Atom ID"), atom_id2: str = Query(..., description="Atom ID 2")):
    query = f"SELECT T1.molecule_id FROM bond AS T1 INNER JOIN connected AS T2 ON T1.bond_id = T2.bond_id WHERE T2.atom_id = ? AND T2.atom_id2 = ?"
    cursor.execute(query, (atom_id, atom_id2))
    result = cursor.fetchall()
    return result

# Endpoint to get element based on atom_id
@app.get("/v1/bird/toxicology/element_by_atom_id", summary="Get element based on atom_id")
async def get_element_by_atom_id(atom_id: str = Query(..., description="Atom ID")):
    query = f"SELECT T.element FROM atom AS T WHERE T.atom_id = ?"
    cursor.execute(query, (atom_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get label based on molecule_id
@app.get("/v1/bird/toxicology/label_by_molecule_id", summary="Get label based on molecule_id")
async def get_label_by_molecule_id(molecule_id: str = Query(..., description="Molecule ID")):
    query = f"SELECT label FROM molecule AS T WHERE T.molecule_id = ?"
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of bonds with specific bond_type
@app.get("/v1/bird/toxicology/percentage_bonds_by_bond_type", summary="Get percentage of bonds with specific bond_type")
async def get_percentage_bonds_by_bond_type(bond_type: str = Query(..., description="Bond type")):
    query = f"SELECT CAST(COUNT(CASE WHEN T.bond_type = ? THEN T.bond_id ELSE NULL END) AS REAL) * 100 / COUNT(T.bond_id) FROM bond t"
    cursor.execute(query, (bond_type,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of distinct molecule_id based on element and label
@app.get("/v1/bird/toxicology/count_distinct_molecule_id_by_element_label", summary="Get count of distinct molecule_id based on element and label")
async def get_count_distinct_molecule_id_by_element_label(element: str = Query(..., description="Element type"), label: str = Query(..., description="Label type")):
    query = f"SELECT COUNT(DISTINCT T1.molecule_id) FROM molecule AS T1 INNER JOIN atom AS T2 ON T1.molecule_id = T2.molecule_id WHERE T2.element = ? AND T1.label = ?"
    cursor.execute(query, (element, label))
    result = cursor.fetchall()
    return result

# Endpoint to get distinct molecule_id based on element and bond_type
@app.get("/v1/bird/toxicology/molecule_id_by_element_bond_type", summary="Get distinct molecule_id based on element and bond_type")
async def get_molecule_id_by_element_bond_type(element: str = Query(..., description="Element type"), bond_type: str = Query(..., description="Bond type")):
    query = f"SELECT DISTINCT T1.molecule_id FROM atom AS T1 INNER JOIN bond AS T2 ON T1.molecule_id = T2.molecule_id WHERE T1.element = ? AND T2.bond_type = ?"
    cursor.execute(query, (element, bond_type))
    result = cursor.fetchall()
    return result

# Endpoint to get molecule_id with more than a certain number of atoms
@app.get("/v1/bird/toxicology/molecule_id_by_atom_count", summary="Get molecule_id with more than a certain number of atoms")
async def get_molecule_id_by_atom_count(label: str = Query(..., description="Label type"), atom_count: int = Query(..., description="Number of atoms")):
    query = f"SELECT T.molecule_id FROM ( SELECT T1.molecule_id, COUNT(T2.atom_id) FROM molecule AS T1 INNER JOIN atom AS T2 ON T1.molecule_id = T2.molecule_id WHERE T1.label = ? GROUP BY T1.molecule_id HAVING COUNT(T2.atom_id) > ? ) t"
    cursor.execute(query, (label, atom_count))
    result = cursor.fetchall()
    return result

# Endpoint to get element based on molecule_id and bond_type
@app.get("/v1/bird/toxicology/element_by_molecule_id_bond_type", summary="Get element based on molecule_id and bond_type")
async def get_element_by_molecule_id_bond_type(molecule_id: str = Query(..., description="Molecule ID"), bond_type: str = Query(..., description="Bond type")):
    query = f"SELECT T1.element FROM atom AS T1 INNER JOIN bond AS T2 ON T1.molecule_id = T2.molecule_id WHERE T1.molecule_id = ? AND T2.bond_type = ?"
    cursor.execute(query, (molecule_id, bond_type))
    result = cursor.fetchall()
    return result

# Endpoint to get molecule_id with the highest number of atoms
@app.get("/v1/bird/toxicology/molecule_id_with_highest_atom_count", summary="Get molecule_id with the highest number of atoms")
async def get_molecule_id_with_highest_atom_count(label: str = Query(..., description="Label type")):
    query = f"SELECT T.molecule_id FROM ( SELECT T2.molecule_id, COUNT(T1.atom_id) FROM atom AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T2.label = ? GROUP BY T2.molecule_id ORDER BY COUNT(T1.atom_id) DESC LIMIT 1 ) t"
    cursor.execute(query, (label,))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of molecules with specific label and bond_type
@app.get("/v1/bird/toxicology/percentage_molecules_by_label_bond_type", summary="Get percentage of molecules with specific label and bond_type")
async def get_percentage_molecules_by_label_bond_type(label: str = Query(..., description="Label type"), bond_type: str = Query(..., description="Bond type"), element: str = Query(..., description="Element type")):
    query = f"SELECT CAST(SUM(CASE WHEN T1.label = ? THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(DISTINCT T1.molecule_id) FROM molecule AS T1 INNER JOIN atom AS T2 ON T1.molecule_id = T2.molecule_id INNER JOIN bond AS T3 ON T1.molecule_id = T3.molecule_id WHERE T3.bond_type = ? AND T2.element = ?"
    cursor.execute(query, (label, bond_type, element))
    result = cursor.fetchall()
    return result

# Endpoint to get count of molecules with specific label
@app.get("/v1/bird/toxicology/count_molecules_by_label", summary="Get count of molecules with specific label")
async def get_count_molecules_by_label(label: str = Query(..., description="Label type")):
    query = f"SELECT COUNT(T.molecule_id) FROM molecule AS T WHERE T.label = ?"
    cursor.execute(query, (label,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of distinct molecule_id based on molecule_id range and bond_type
@app.get("/v1/bird/toxicology/count_distinct_molecule_id_by_range_bond_type", summary="Get count of distinct molecule_id based on molecule_id range and bond_type")
async def get_count_distinct_molecule_id_by_range_bond_type(start_molecule_id: str = Query(..., description="Start molecule ID"), end_molecule_id: str = Query(..., description="End molecule ID"), bond_type: str = Query(..., description="Bond type")):
    query = f"SELECT COUNT(DISTINCT T.molecule_id) FROM bond AS T WHERE T.molecule_id BETWEEN ? AND ? AND T.bond_type = ?"
    cursor.execute(query, (start_molecule_id, end_molecule_id, bond_type))
    result = cursor.fetchall()
    return result

# Endpoint to get count of atoms based on molecule_id and element
@app.get("/v1/bird/toxicology/count_atoms_by_molecule_id_element", summary="Get count of atoms based on molecule_id and element")
async def get_count_atoms_by_molecule_id_element(molecule_id: str = Query(..., description="Molecule ID"), element: str = Query(..., description="Element type")):
    query = f"SELECT COUNT(T.atom_id) FROM atom AS T WHERE T.molecule_id = ? AND T.element = ?"
    cursor.execute(query, (molecule_id, element))
    result = cursor.fetchall()
    return result

# Endpoint to get element based on atom_id and label
@app.get("/v1/bird/toxicology/element_by_atom_id_label", summary="Get element based on atom_id and label")
async def get_element_by_atom_id_label(atom_id: str = Query(..., description="Atom ID"), label: str = Query(..., description="Label type")):
    query = f"SELECT T1.element FROM atom AS T1 INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id WHERE T1.atom_id = ? AND T2.label = ?"
    cursor.execute(query, (atom_id, label))
    result = cursor.fetchall()
    return result


# Endpoint to get count of distinct molecule_id based on bond_type and element
@app.get("/v1/bird/toxicology/count_distinct_molecule_ids", summary="Get count of distinct molecule IDs based on bond type and element")
async def get_count_distinct_molecule_ids(bond_type: str = Query(..., description="Type of bond"), element: str = Query(..., description="Element type")):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT COUNT(DISTINCT T1.molecule_id)
    FROM atom AS T1
    INNER JOIN bond AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T2.bond_type = ? AND T1.element = ?
    """
    cursor.execute(query, (bond_type, element))
    result = cursor.fetchone()
    conn.close()
    return {"count": result[0]}

# Endpoint to get count of distinct molecule_id based on bond_type and label
@app.get("/v1/bird/toxicology/count_distinct_molecule_ids_by_label", summary="Get count of distinct molecule IDs based on bond type and label")
async def get_count_distinct_molecule_ids_by_label(bond_type: str = Query(..., description="Type of bond"), label: str = Query(..., description="Label type")):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT COUNT(DISTINCT T1.molecule_id)
    FROM molecule AS T1
    INNER JOIN bond AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T2.bond_type = ? AND T1.label = ?
    """
    cursor.execute(query, (bond_type, label))
    result = cursor.fetchone()
    conn.close()
    return {"count": result[0]}

# Endpoint to get distinct elements and bond types for a given molecule_id
@app.get("/v1/bird/toxicology/distinct_elements_bond_types", summary="Get distinct elements and bond types for a given molecule ID")
async def get_distinct_elements_bond_types(molecule_id: str = Query(..., description="Molecule ID")):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT DISTINCT T1.element, T2.bond_type
    FROM atom AS T1
    INNER JOIN bond AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T1.molecule_id = ?
    """
    cursor.execute(query, (molecule_id,))
    result = cursor.fetchall()
    conn.close()
    return {"elements_bond_types": result}

# Endpoint to get atom_id based on molecule_id, bond_type, and element
@app.get("/v1/bird/toxicology/atom_ids", summary="Get atom IDs based on molecule ID, bond type, and element")
async def get_atom_ids(molecule_id: str = Query(..., description="Molecule ID"), bond_type: str = Query(..., description="Type of bond"), element: str = Query(..., description="Element type")):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT T1.atom_id
    FROM atom AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    INNER JOIN bond AS T3 ON T2.molecule_id = T3.molecule_id
    WHERE T2.molecule_id = ? AND T3.bond_type = ? AND T1.element = ?
    """
    cursor.execute(query, (molecule_id, bond_type, element))
    result = cursor.fetchall()
    conn.close()
    return {"atom_ids": result}

# Endpoint to get atom_id based on element and label
@app.get("/v1/bird/toxicology/atom_ids_by_element_label", summary="Get atom IDs based on element and label")
async def get_atom_ids_by_element_label(element: str = Query(..., description="Element type"), label: str = Query(..., description="Label type")):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT T1.atom_id
    FROM atom AS T1
    INNER JOIN molecule AS T2 ON T1.molecule_id = T2.molecule_id
    WHERE T1.element = ? AND T2.label = ?
    """
    cursor.execute(query, (element, label))
    result = cursor.fetchall()
    conn.close()
    return {"atom_ids": result}