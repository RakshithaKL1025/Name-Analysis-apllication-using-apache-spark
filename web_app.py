from flask import Flask, render_template, request, jsonify, send_file
import os
import json
from datetime import datetime

app = Flask(__name__)

# Import our name counting functions
from local_name_counter import count_names_with_letter, count_letter_occurrences, save_results_to_file

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/count', methods=['POST'])
def count_names():
    try:
        # Get data from form
        names_text = request.form.get('names', '')
        target_letter = request.form.get('letter', '')
        
        # Process names (split by comma or newline)
        names_list = []
        if names_text:
            # Split by comma or newline and clean up
            names_list = [name.strip() for name in names_text.replace('\n', ',').split(',') if name.strip()]
        
        if not names_list:
            return jsonify({'error': 'Please enter at least one name'}), 400
            
        if not target_letter:
            return jsonify({'error': 'Please enter a target letter'}), 400
        
        # Perform counting
        matching_names = [name for name in names_list if target_letter.lower() in name.lower()]
        name_count = len(matching_names)
        total_occurrences = sum(name.lower().count(target_letter.lower()) for name in names_list)
        
        # Prepare results
        results = {
            'names_processed': len(names_list),
            'target_letter': target_letter,
            'matching_names': matching_names,
            'name_count': name_count,
            'total_occurrences': total_occurrences,
            'all_names_details': [{'name': name, 'occurrences': name.lower().count(target_letter.lower())} 
                                for name in names_list]
        }
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/save_results', methods=['POST'])
def save_results():
    try:
        # Get data from request
        data = request.get_json()
        
        # Extract fields
        names_list = data.get('names_list', [])
        target_letter = data.get('target_letter', '')
        
        if not names_list or not target_letter:
            return jsonify({'error': 'Missing required data'}), 400
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"name_count_results_{timestamp}.txt"
        
        # Save results using our existing function
        save_results_to_file(names_list, target_letter, filename)
        
        # Return success with filename
        return jsonify({'success': True, 'filename': filename})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/download/<filename>')
def download_file(filename):
    try:
        return send_file(filename, as_attachment=True)
    except Exception as e:
        return str(e), 404

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    if not os.path.exists('templates'):
        os.makedirs('templates')
    
    app.run(debug=True, host='0.0.0.0', port=5000)