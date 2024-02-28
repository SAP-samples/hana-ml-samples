import numpy as np
import matplotlib.pyplot as plt
from hana_ml.visualizers.metrics import MetricsVisualizer
from hana_ml.algorithms.pal.metrics import confusion_matrix, binary_classification_debriefing
from hana_ml.dataframe import  quotename

def plot_selection_rate(data, vars, label, x_label):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
    get_selection_rate(ax1, data, vars[0], label, x_label[0])
    get_selection_rate(ax2, data, vars[1], label, x_label[1])
    #plt.savefig(f'selection rate.png')
    plt.show()

def get_selection_rate(ax, data, var, label, x_label):
    sr_vals = []
    for i in [0, 1]:
        group = data.filter(f'"{var}" = \'{i}\'')
        #print(group.select_statement)
        ratio_df = group.agg([('count', 'ID', 'Number')], group_by=label).sort(label).collect()
        #display(ratio_df)
        sr = ratio_df.iat[1,1]/(ratio_df.iat[0,1] + ratio_df.iat[1,1])
        sr_vals.append(sr)
    x1 = np.arange(len(sr_vals))
    ax.bar(x1, sr_vals, label="selection rate")
    ax.text(x1[0], sr_vals[0], str(round(sr_vals[0], 3)), ha='center', va='bottom')
    ax.text(x1[1], sr_vals[1], str(round(sr_vals[1], 3)), ha='center', va='bottom')
    ax.set_ylim([0.0, 1])
    ax.set_xticks([0, 1])
    ax.set_xlabel(var)
    ax.set_xticklabels(x_label)
    ax.legend()
    ax.set_title(f'Selection rate of groups of "{var}"')
    return ax

def plot_metric_comparision_by_var(true_data, predict_data, sensitive_variable, group_names, key, label, 
                                   positive_label, negative_label, title, x_label):
    dict_0 = get_metrics(true_data, predict_data, sensitive_variable, group_names[0], key, label, positive_label, negative_label, x_label)
    #print(dict_0)
    dict_1 = get_metrics(true_data, predict_data, sensitive_variable, group_names[1], key, label, positive_label, negative_label, x_label)
    #print(dict_1)
    plot_metrics(dict_0, dict_1, title, sensitive_variable, group_names, x_label)
    return dict_0, dict_1

def get_metrics(true_data, predict_data, sensitive_variable, group_name, key, label, positive_label, negative_label, x_label):
    group_df = true_data.filter(f'"{sensitive_variable}"= \'{group_name}\'')
    join_group = group_df.rename_columns({key:"ID_TRUE", label:"ORIGINAL"}).join(predict_data, f"ID_TRUE={key}").select([key, "ORIGINAL", "SCORE"])
    return (binary_classification_debriefing(join_group, label_true="ORIGINAL", label_pred="SCORE",
                                             positive_label=positive_label, negative_label=negative_label))

def plot_metrics(dict_0, dict_1, title, sensitive_variable, group_names, x_label):
    metrics = ['accuracy', 'precision', 'recall', 'selection_rate', 'false_positive_rate']
    data = [
        ((np.arange(1), dict_0['ACCURACY']),  (np.arange(1), dict_1['ACCURACY'])),
        ((np.arange(1), dict_0['PRECISION']), (np.arange(1), dict_1['PRECISION'])),
        ((np.arange(1), dict_0['RECALL']),    (np.arange(1), dict_1['RECALL'])),
        ((np.arange(1), dict_0['SELECTION RATE']), (np.arange(1), dict_1['SELECTION RATE'])),
        ((np.arange(1), dict_0['FPR']), (np.arange(1), dict_1['FPR']))         ]

    fig, axs = plt.subplots(2, 3, figsize=(10, 5))
    width = 0.2

    for i, ((x1, y1), (x2, y2)) in enumerate(data):
        row = i // 3
        col = i % 3
        axs[row, col].bar(x1 - width, y1, width, color='b', alpha=0.5)
        axs[row, col].bar(x2 + width, y2, width, color='r', alpha=0.5)
        axs[row, col].text(x1 - width, y1, str(round(y1, 3)), ha='center', va='bottom')
        axs[row, col].text(x2 + width, y2, str(round(y2, 3)), ha='center', va='bottom')
        axs[row, col].set_title(metrics[i])
        axs[row, col].set_ylim([0, 1.05])
        #axs[row, col].xaxis.set_visible(False)
        axs[row, col].set_xlabel(sensitive_variable)
        axs[row, col].set_xticks([0-width,  0.0, 0+width])
        axs[row, col].set_xticklabels([x_label[0], '', x_label[1]])
    axs[1, 2].axis('off')

    fig.suptitle(title)
    plt.tight_layout()
    #plt.savefig(title)
    plt.show()

def print_metrics(group_name, result_dict):
    print(f"Metrics for group '{group_name}' : ")
    print('ACCURACY : ', result_dict['ACCURACY'])
    print('RECALL[TPR] : ', result_dict['RECALL'] ) #equalized_odds, true_positive_rate_parity
    print('PRECISION : ', result_dict['PRECISION']) #error_rate_parity
    print('FPR : ', result_dict['FPR']) #equalized_odds, false_positive_rate_parity
    print('SELECTION RATE : ', result_dict['SELECTION RATE']) #demographic_parity
    print('\n')

def create_comparison_plot(ax, metric, dict_0, dict_0_fm, dict_1, dict_1_fm, variable, x_label):
    group0 = [dict_0[metric], dict_1[metric]]
    group1 = [dict_0_fm[metric], dict_1_fm[metric]]
    bar_width = 0.3
    x1 = np.arange(len(group0))
    x2 = [i + bar_width for i in x1]
    ax.bar(x1, group0, bar_width, label='"regular" HGBT model')
    ax.bar(x2, group1, bar_width, label='FairML HGBT model with demographic_parity')
    for i, v in enumerate(group0):
        ax.text(i, v + 0.05, "{:.3f}".format(v), ha='center', va='bottom')
    for i, v in enumerate(group1):
        ax.text(i + bar_width, v + 0.05, "{:.3f}".format(v), ha='center', va='bottom')
    ax.set_ylim([0.0, 1.2])
    ax.set_xticks([0.15, 1.15])
    ax.set_xlabel(variable) 
    ax.set_xticklabels(x_label)
    ax.legend()
    ax.set_title(f'{metric} splited by {variable}')
    #plt.savefig(f'{metric} splited by {variable}')

def model_comparison_plot(metrics, dict_0, dict_0_fm, dict_1, dict_1_fm, variable, x_label):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 5))
    create_comparison_plot(ax1, metrics[0], dict_0, dict_0_fm, dict_1, dict_1_fm, variable, x_label)
    create_comparison_plot(ax2, metrics[1], dict_0, dict_0_fm, dict_1, dict_1_fm, variable, x_label)
    #plt.savefig('model comparison plot')
    plt.show()

def cm_compare(original_data, score_predictions, sensitive_variable, label, title):
    cmd_1=original_data.filter(f'"{sensitive_variable}"=\'1\'').select('ID', label).rename_columns(["ID", "ACTUAL_CLASS"]).set_index("ID").join(score_predictions.select('ID', 'SCORE').rename_columns(["ID", "PREDICTED_CLASS"]).set_index("ID"))
    cmd_0=original_data.filter(f'"{sensitive_variable}"=\'0\'').select('ID', label).rename_columns(["ID", "ACTUAL_CLASS"]).set_index("ID").join(score_predictions.select('ID', 'SCORE').rename_columns(["ID", "PREDICTED_CLASS"]).set_index("ID"))
    #cmd_M.head(2).collect()
    cm1, cr1 = confusion_matrix(data=cmd_1, key='ID', label_true='ACTUAL_CLASS', label_pred='PREDICTED_CLASS') 
    cm0, cr0 = confusion_matrix(data=cmd_0, key='ID', label_true='ACTUAL_CLASS', label_pred='PREDICTED_CLASS') 
    #cm.collect()
    
    cmD=cm1.set_index(["ACTUAL_CLASS", "PREDICTED_CLASS"]).join(
        cm0.rename_columns(["AC", "PC", "COUNT2"]).set_index(["AC", "PC"])).select("ACTUAL_CLASS", "PREDICTED_CLASS", ('COUNT - COUNT2', 'COUNT'))

    f = plt.figure(figsize=(20,5))
    subpltID = 131
    plot1 = f.add_subplot(subpltID)
    type1 = title['1']
    mv = MetricsVisualizer(plot1, enable_plotly=False, title=f'{type1} applicants')
    xx = mv.plot_confusion_matrix(cm1, normalize=False)
    
    subpltID = 132
    plot2 = f.add_subplot(subpltID)
    type0 = title['0']
    mv = MetricsVisualizer(plot2, enable_plotly=False,  title=f'{type0} applicants')
    xx = mv.plot_confusion_matrix(cm0, normalize=False)
     
    subpltID = 133
    plot3 = f.add_subplot(subpltID)
    mv = MetricsVisualizer(plot3, enable_plotly=False, cmap='Purples', title='Difference')
    xx = mv.plot_confusion_matrix(cmD , normalize=False)