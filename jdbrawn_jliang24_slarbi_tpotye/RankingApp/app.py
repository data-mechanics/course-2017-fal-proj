from flask import Flask, request, redirect, flash, url_for
from flask import render_template
from rankingOptimizer import rankingOptimizer
import dml

app= Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/ChooseK/', methods=['GET', 'POST'])
def show_result():
    try:
        k= request.form['searchValue']
        schools=['Massachusetts General Hospital Dietetic Internship','Suffolk University', 'Benjamin Franklin Institute of Technology','Bunker Hill Community College','MGH Institute of Health Professions','Emmanuel College',
  'School of the Museum of Fine Arts-Boston','Simmons','Boston University','The Boston Conservatory','Wheelock College','Roxbury Community College','University of Massachusetts-Boston','Boston University Research',
  'Boston University School of Law','Boston University Sargent College','Boston University School of Management','Boston University Admissions','Boston University Rental Office','Boston University Trustees',
  'Boston University School of Medicine','Boston University Public Health','Boston College','Simmons College','Harvard Medical School','Tufts University School of Medicine','MASCO Colleges of the Fenway','Harvard University Sailing',
  'Harvard Business School','New England College of Business and Finance','North Bennet Street School','Empire Beauty School-Boston','Urban College of Boston','Bay State College','Butera School Of Art','Emerson College',
  'Fisher College','Sanford-Brown College-Boston','New England School of Law','Kaplan Career Institute (Closed)','Boston Architectural College','Massachusetts College of Art and Design','MCPHS University','New England College of Optometry',
  'Northeastern University','The New England Conservatory of Music','Wentworth Institute of Technology','Berklee College of Music','Kaplan Career Institute (Closed)','New England School of Photography','Art Institute of Boston at Lesley University',
  'Laboure College','Everest Institute-Brighton','Saint Johns Seminary','Pine Manor College','Massachusetts School of Professional Psychology','Boston Baptist College','Blaine The Beauty Career School-Boston','Rets Technical Center',
  'Harvard University of Public Health']
        if k not in schools:
            flash('Please enter a valid school')
            return redirect(url_for('index'))
        else:
            result= rankingOptimizer(k)
            print("School Input: " + result.school)
            answer= rankingOptimizer.execute(result)
            return render_template('results.html', improvement=answer)
        
    except Exception as e:
        flash('Please enter a valid school')
        return redirect(url_for('index'))

if __name__ == "__main__":
    app.secret_key= 'super secret key'
    app.config['SESSION_TYPE']= 'filesystem'
    app.run(debug=True)

