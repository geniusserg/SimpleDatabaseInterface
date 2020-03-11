from tkinter import *
from DataBase import DataBase

#GLOBAL VARIABLES

database = '' #DataBase

file =  ""

columna_text = "column A"
columnb_text = "column B"
columnc_text = "column C"

class Window(object):
        def __init__(self,window):
        #INIT DATABASE
                global database
                database = DataBase(file, columna_text, columnb_text, columnc_text)
                #database.LoadFromCSV()
                if database.lastError == 1:
                        print("file does`nt exist!")
                        exit(-1)
                        return 0
        #INIT WINDOW
                self.window = window
                self.window.wm_title("Data Base Sergey Danilov Lab 1")
                l1 = Label(window, text="$ID: ")
                l1.grid(row=0, column=0)
                l2 = Label(window, text=database.columnA)
                l2.grid(row=0, column=2)
                l3 = Label(window, text=database.columnB)
                l3.grid(row=1, column=0)
                l4 = Label(window, text=database.columnC)
                l4.grid(row=1, column=2)
                self.ID = StringVar()
                self.e1 = Entry(window, textvariable=self.ID)
                self.e1.grid(row=0, column=1)
                self.columnA = StringVar()
                self.e2 = Entry(window, textvariable=self.columnA)
                self.e2.grid(row=0, column=3)
                self.columnB = StringVar()
                self.e3 = Entry(window, textvariable=self.columnB)
                self.e3.grid(row=1, column=1)
                self.columnC = StringVar()
                self.e4= Entry(window, textvariable=self.columnC)
                self.e4.grid(row=1, column=3)
        #INIT TABLE
                self.list = Listbox(window, height=10, width=50)
                self.list.grid(row=2, column=0, rowspan=8, columnspan=2)
                self.list.bind('<<ListboxSelect>>', self.get_selected_row)
                sb1 = Scrollbar(window)
                sb1.grid(row=2, column=2, rowspan=8)
                self.list.config(yscrollcommand=sb1.set)
                sb1.config(command=self.list.yview)
        #INIT BUTTON
                b3 = Button(window, text="Append", width=12, command=self.add_command)
                b3.grid(row=2, column=3)
                b4 = Button(window, text="Update", width=12, command=self.update_command)
                b4.grid(row=3, column=3)
                b5 = Button(window, text="Delete", width=12, command=self.delete_command)
                b5.grid(row=4, column=3)
                b5 = Button(window, text="Clear", width=12, command=self.clear_command)
                b5.grid(row=5, column=3)                
                b6 = Button(window, text="Save", width=12, command=self.save_db)
                b6.grid(row=6, column=3)
                b7 = Button(window, text="BackUp", width=12, command=self.load_backup)
                b7.grid(row=7, column=3)
                self.view_command()
#WRAPPER FUNCTIONS UNDER DATABASE

        def clear_command(self):
                database.SaveBackUp()
                database.ClearDB()
                self.view_command()
                
        def get_selected_row(self,event=""):   
                try:
                        index = self.list.curselection()[0]
                        self.selected_tuple = self.list.get(index)
                        return self.selected_tuple[0]
                except IndexError:
                        return -1

        def view_command(self):
                global database
                self.list.delete(0,END)
                if len(database.table) == 0:
                        return 0
                for row in database.table.keys():
                        self.list.insert(END,(row, database.table[row][columna_text], database.table[row][columnb_text], database.table[row][columnc_text]))

        def add_command(self):
                global database
                database.SaveBackUp()
                if self.ID.get() != "":
                        database.AppendRow(self.ID.get(), self.columnA.get(), self.columnB.get(), self.columnC.get())
                if database.lastError == 1:
                        print('ERROR')
                        database.lastError = 0
                        return 0
                self.view_command()

        def find_and_delete_command(self):
                set_to_delete = set()
                database.SaveBackUp()
                if (self.ID.get() != ""):
                        database.DeleteByID(self.ID.get())
                else:
                        if (self.columnA.get() != ""):
                                set_to_delete.update(list(database.FindRows(columna_text, self.columnA.get())))
                        if (self.columnB.get() != ""):
                                set_to_delete.update(list(database.FindRows(columnb_text, self.columnB.get())))
                        if (self.columnC.get() != ""):
                                set_to_delete.update(list(database.FindRows(columnc_text, self.columnC.get())))
                        for i in set_to_delete:
                                database.DeleteByID(i)

        def delete_command(self):
                global database
                database.SaveBackUp()
                id_selected = self.get_selected_row()
                if id_selected == -1:
                        self.find_and_delete_command()
                else:
                        try:
                                database.DeleteByID(id_selected[0])
                        except:
                                pass
                self.view_command()

        def update_command(self):
                global database
                database.SaveBackUp()
                if self.get_selected_row() != -1:
                        try:
                                database.UpdateRow(self.selected_tuple[0], {"ID": self.ID.get(), columna_text: self.columnA.get(),columnb_text:self.columnB.get(), columnc_text:self.columnC.get()})
                        except:
                                pass
                self.view_command()

        def save_db(self):
                global database
                database.SaveDBtoCSV()
                self.view_command()

        def load_backup(self):
                global database
                database.LoadFromBackUp()
                self.view_command()
                
#END CLASS WINDOW        


#MAIN
if __name__ == "__main__":
        print("enter database filename")
        file = input()
        window = Tk()
        window.geometry('{}x{}'.format(500, 200))
        Window(window)
        window.mainloop()
